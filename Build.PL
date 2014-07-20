use 5.006;
use strict;
use warnings FATAL => 'all';
use Module::Build;

my $builder = Module::Build->new(
    module_name         => 'SemanticQuery',
    license             => 'Artistic_2_0',
    dist_author         => q{Eli Wenig <eli@csh.rit.edu>},
    dist_version_from   => 'lib/SemanticQuery.pm',
    release_status      => 'development',
    configure_requires => {
        'Module::Build' => 0,
    },
    build_requires => {
        'Test::More' => 0,
    },
    requires => {
		'Log::Contextual' => 0.006003,
		'Log::Contextual::SimpleLogger' => 0.006003,
		'Carp' => 1.3301,
		'URL::Encode' => 0.03,
		'MongoDB' => v0.704.2.0,
		'Mouse' => 2.3.0,
		'Storable' => 2.27,
		'Digest::MD5' => 2.51,
		'ZMQ::LibZMQ3' => 1.17,
		'IO::Socket' => 1.32,
		'JSON' => 2.53,
    },
    add_to_cleanup     => [ 'SemanticQuery-*' ],
    create_makefile_pl => 'traditional',
);

$builder->create_build_script();
