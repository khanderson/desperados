# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name        = "desperados"
  s.version     = "0.2"
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["John Leach", "Karl Anderson"]
  s.email       = ["john@johnleach.co.uk.co.uk"]
  s.homepage    = "https://github.com/khanderson/desperados"
  s.summary     = %q{Ruby library for interacting with Ceph's RADOS v2}
  s.description = %q{Ruby library for Ceph's Reliable Autonomic Distributed Object Store. Wraps the C++ librados library with Ruby love. }

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.require_paths = ["lib"]

	s.add_dependency 'ffi', "~> 1.0.0"

end
