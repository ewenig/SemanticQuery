#!perl -T
use 5.006;
use strict;
use warnings FATAL => 'all';
use Test::More;

plan tests => 1;

BEGIN {
    use_ok( 'TextWise::Categorizer' ) || print "Bail out!\n";
}

diag( "Testing TextWise::Categorizer $TextWise::Categorizer::VERSION, Perl $], $^X" );
