require_relative 'lib/oceanbase_adapter/version'

Gem::Specification.new do |spec|
  spec.name          = "oceanbase_adapter"
  spec.version       = OceanbaseAdapter::VERSION
  spec.authors       = ["zhangcaiyan"]
  spec.email         = ["zhangcaiyanbeyond@gmail.com"]

  spec.summary       = %q{Oceanbase client}
  spec.description   = %q{Oceanbase client}
  spec.homepage      = "https://github.com/zhangcaiyan"
  spec.license       = "MIT"
  spec.required_ruby_version = Gem::Requirement.new(">= 2.3.0")

  spec.metadata["allowed_push_host"] = "https://github.com/zhangcaiyan"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/zhangcaiyan"
  spec.metadata["changelog_uri"] = "https://github.com/zhangcaiyan"

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files         = Dir.chdir(File.expand_path('..', __FILE__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]
end
