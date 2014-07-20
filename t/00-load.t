#!perl -T
use 5.006;
use strict;
use warnings FATAL => 'all';
use Test::More;

plan tests => 1;

BEGIN {
    use_ok( 'SemanticQuery::Categorizer' ) || print "Bail out!\n";
}

diag( "Testing SemanticQuery::Categorizer $SemanticQuery::Categorizer::VERSION, Perl $], $^X" );
