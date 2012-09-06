require 'ffi'

module Rados #:nodoc:
  module Lib #:nodoc:
    extend FFI::Library

    # Some quasi-typedefs for clarity, I can't get Library#typedef to work at all.
    RADOS_T = :pointer
    RADOS_IOCTX_T = :pointer
 
    ffi_lib 'rados.so.2'

    attach_function 'rados_create', [:pointer, :string], :int
    attach_function 'rados_shutdown', [RADOS_T], :void
    attach_function 'rados_conf_read_file', [RADOS_T, :string], :int
    attach_function 'rados_conf_set', [RADOS_T, :string, :string], :int
    attach_function 'rados_connect', [RADOS_T], :int
    attach_function 'rados_ioctx_create', [RADOS_T, :string, :pointer], :int
    attach_function 'rados_ioctx_destroy', [RADOS_IOCTX_T], :void
    attach_function 'rados_write', [RADOS_IOCTX_T, :string, :buffer_in, :size_t, :off_t], :int
    attach_function 'rados_write_full', [RADOS_IOCTX_T, :string, :buffer_in, :size_t], :int
    attach_function 'rados_read', [RADOS_IOCTX_T, :string, :pointer, :size_t, :off_t], :int
    attach_function 'rados_remove', [RADOS_IOCTX_T, :string], :int
    attach_function 'rados_pool_create', [RADOS_T, :string], :int
    attach_function 'rados_pool_delete', [RADOS_T, :string], :int
  end

  class RadosError < StandardError ; end
  class PoolNotFound < RadosError ; end
  class WriteError < RadosError ; end
  class ShortWriteError < WriteError ; end
  class ReadError < RadosError ; end
  class ObjectNotFound < ReadError ; end

  # Initialize the Rados library. Must be called once before any other
  # operations.
  def self.initialize(options = {})
    # Handle some options explictly, others get forwarded to rados_conf_set.
    user, conf_file = options.delete(:user), options.delete(:config_file)

    unless initialized?
      handle_ptr = FFI::MemoryPointer.new Lib::RADOS_T
      trap_error("initializing rados") { Lib.rados_create(handle_ptr, user) }
      handle = handle_ptr.get_pointer(0)

      begin
        conf_file_desc = conf_file ? conf_file.inspect : "from default location"

        # NB. passing NULL to rados_conf_read_file makes librados use defaults.
        trap_error("loading config file #{conf_file_desc}") do
          Lib.rados_conf_read_file(handle, conf_file)
        end

        # Let explicit config options override values from the config file.
        options.each do |option, value|
          trap_error("applying config option '#{option}'",
                     Errno::ENOENT::Errno => "'#{option}' is not a valid option") do
            Lib.rados_conf_set(handle, option.to_s, value.to_s)
          end
        end

        trap_error("connecting to rados") { Lib.rados_connect(handle) }

        @handle_ptr = handle_ptr
      rescue
        Lib.rados_shutdown(handle)
        raise
      end
    end
  end

  def self.handle
    @handle_ptr or raise RadosError, "Not connected"
    @handle_ptr.get_pointer(0)
  end

  # A Rados::Object represents an object in a pool in a cluster.
  class Object
    attr_reader :id, :position, :pool

    def initialize(attributes = {})
      @id = attributes[:id]
      @pool = attributes[:pool]
      @position = 0
    end

    def write(buf)
      # FIXME: If the following does a part write and raises an
      # exception then the @position is invalid
      @position += pool.write(id, buf, :offset => @position)
      self
    end

    def read
      # FIXME: If the following does a part read and raises an
      # exception then the @position is invalid
      buf = pool.read(id, :offset => @position)
      if buf.size == 0
        nil
      else
        @position += buf.size
        buf
      end
    end

    def seek(amount, whence = IO::SEEK_SET)
      case whence
      when IO::SEEK_SET
        @position = amount
      when IO::SEEK_CUR
        @position += amount
      end
    end

  end

  class ObjectCollection

    attr_reader :pool

    def initialize(pool)
      @pool = pool
    end

    def find(oid)
      Object.new(:id => oid, :pool => pool)
    end

    def new(oid = nil)
      Object.new(:id => oid, :pool => pool)
    end

  end

  # Represents a Pool in the cluster.  Use Pool.find to get a pool from the
  # cluster and Pool.create to create new ones.
  class Pool

    # The name of this pool
    attr_reader :name
    # The objects in this pool. See Rados::ObjectCollection.
    attr_reader :objects

    def initialize(name) #:nodoc:
      @name = name
      @objects = ObjectCollection.new(self)
    end

    # Get the named pool from the cluster. Returns a Pool
    # instance. Raises Rados::PoolNotFound if Pool doesn't exist
    def self.find(name)
      p = new(name)
      p.pool
      p
    end

    def self.handle
      Rados.handle
    end

    # Create a new pool in the cluster with the given name. Returns a
    # Pool instance. Raises a RadosError exception if the pool could
    # not be created.
    def self.create(name)
      ret = Lib.rados_pool_create(handle, name)
      if ret < 0
        raise RadosError, "creating pool #{name}"
      else
        new(name)
      end
    end

    # The internal Rados pool data stucture
    def pool #:nodoc:
      pool_ptr or raise RadosError, "Pool is not initialized"
      pool_ptr.get_pointer(0)
    end

    def pool_ptr
      @pool_ptr ||= begin
        pool_ptr = FFI::MemoryPointer.new Lib::RADOS_IOCTX_T

        ret = Lib.rados_ioctx_create self.class.handle, name, pool_ptr
        if ret < 0
          raise PoolNotFound, name
        end

        pool_ptr
      end
    end

    # Destroy the pool. Raises a RadosError exception if the pool
    # could not be deleted.
    def destroy
      sanity_check "destroy already"
      ret = Lib.rados_pool_delete(self.class.handle, @name)
      if ret < 0
        raise RadosError, "deleting pool #{name}"
      else
        @destroyed = true
        self
      end
    end

    # Returns true if the pool as been marked as destroyed since
    # instantiated.
    def destroyed?
      @destroyed == true
    end

    # Write the data <tt>buf</tt> to the object id <tt>oid</tt> in the
    # pool.  Both <tt>oid</tt> and <tt>buf</tt> should be
    # strings. Returns the number of bytes written.  If the write
    # fails then a Rados::WriteError exception is raised. If the data
    # was only partly written then a Rados::ShortWriteError exception
    # is raised.
    #
    # Available options are:
    # * <tt>:size</tt> - Number of bytes to write. Defaults to the size of <tt>buf</tt>
    # * <tt>:offset</tt> - The number of bytes from the beginning of the object to start writing at. Defaults to 0.
    def write(oid, buf, options = {})
      sanity_check "write to"
      offset = options.fetch(:offset, 0)
      len = options.fetch(:size, buf.size)
      ret = Lib.rados_write(pool, oid, buf, len, offset)
      if ret < 0
        raise WriteError, "writing #{len} bytes at offset #{offset} to #{oid} in pool #{name}: #{ret}"
      elsif ret < len
        raise ShortWriteError, "writing #{len} bytes to pool #{name} only wrote #{ret}"
      end
      ret
    end

    # Reads object data from the object id <tt>oid</tt> in the pool.
    # Returns the data as a string. If no object with the oid exists, a
    # Rados::ObjectNotFound exception is raised. If the read fails
    # then a Rados::ReadError exception is raised.
    #
    # Available options are:
    # * <tt>:size</tt> - The maximum amount of data to read, defaults to reading the entire object.
    # * <tt>:offset</tt> - The number of bytes from the beginning of the object to start writing at. Defaults to 0.
    # * <tt>:chunk_size</tt> - The number of bytes to read in each call to librados, defaults to 32KB.
    def read(oid, options = {})
      size = options[:size]
      offset = options.fetch(:offset, 0)
      chunk_size = options.fetch(:chunk_size, 32768)

      parts = []

      while size.nil? || size > 0
        more_data = read_part(oid, :offset => offset, :size => [chunk_size, size].compact.min)
        bytes_read = more_data.bytesize
        break if bytes_read == 0

        size -= bytes_read if size
        offset += bytes_read
        parts << more_data
      end

      parts.join
    end

    # Reads up to a fixed number of bytes of data from the object id <tt>oid</tt> in the pool.
    # Returns the data as a string. If no object with the oid exists, a
    # Rados::ObjectNotFound exception is raised. If the read fails
    # then a Rados::ReadError exception is raised.
    #
    # Available options are:
    # * <tt>:size</tt> - The amount of data to read. As objects can be huge, it is unlikely that you'll want to load the entire thing into ram, so defaults to 8192 bytes.
    # * <tt>:offset</tt> - The number of bytes from the beginning of the object to start writing at. Defaults to 0.
    def read_part(oid, options = {})
      sanity_check "read from"
      offset = options.fetch(:offset, 0)
      len = options.fetch(:size, 8192)
      buf = FFI::MemoryPointer.new :char, len
      ret = Lib.rados_read(pool, oid, buf, len, offset)
      if ret == -2
        raise ObjectNotFound, "reading from '#{oid}' in pool #{name}"
      elsif ret < 0
        raise ReadError, "reading #{len} bytes at offset #{offset} from #{oid} in pool #{name}: #{ret}"
      end
      buf.read_string(ret)
    end

    # Deletes the object id <tt>oid</tt> from the pool.  If the object
    # could not be removed then a Rados::RemoveError exception is
    # raised.
    def remove(oid)
      sanity_check "remove from"
      ret = Lib.rados_remove(pool, oid)
      if ret < 0
        raise RemoveError, "removing #{oid} from pool #{name}"
      end
      true
    end

    private

    def sanity_check(action)
      raise RadosError, "attempt to #{action} destroyed pool #{name}" if destroyed?
    end      
  end

  private

  # A wrapper for processing the return value of rados library calls.
  def self.trap_error(context = "accessing rados", message_overrides = {})
    if (ret = yield) < 0
      message = message_overrides.fetch(-ret, SystemCallError.new(-ret).message)
      raise RadosError, "Error while #{context}: #{message}"
    end
  end

  def self.initialized?
    @handle_ptr
  end
end
