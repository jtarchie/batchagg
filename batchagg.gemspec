# frozen_string_literal: true

require_relative "lib/batchagg/version"

Gem::Specification.new do |spec|
  spec.name = "batchagg"
  spec.version = BatchAgg::VERSION
  spec.authors = ["JT Archie"]
  spec.email = ["jtarchie@gmail.com"]

  spec.summary = "Efficiently perform multiple database aggregations on ActiveRecord models in a single query"
  spec.description = "BatchAgg eliminates N+1 query problems when calculating counts, sums, averages, and other aggregates across associations by using correlated subqueries to fetch all aggregations in a single efficient database call."
  spec.homepage = "https://github.com/jtarchie/batchagg."
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.3.0"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = spec.homepage

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  gemspec = File.basename(__FILE__)
  spec.files = IO.popen(%w[git ls-files -z], chdir: __dir__, err: IO::NULL) do |ls|
    ls.readlines("\x0", chomp: true).reject do |f|
      (f == gemspec) ||
        f.start_with?(*%w[bin/ test/ spec/ features/ .git .github appveyor Gemfile])
    end
  end
  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  # Uncomment to register a new dependency of your gem
  spec.add_dependency "activerecord"

  # For more information and examples about making a new gem, check out our
  # guide at: https://bundler.io/guides/creating_gem.html
  spec.metadata["rubygems_mfa_required"] = "true"
end
