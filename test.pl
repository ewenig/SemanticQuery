#!/usr/bin/perl

use warnings;
use strict;
use lib 'lib/';
use TextWise::Categorizer;
use Env qw(TW_API_TOKEN TARGET_URL);
use Data::Dumper qw(Dumper);

my $cr = TextWise::Categorizer->new(API_TOKEN => $TW_API_TOKEN);
my $res = $cr->categorize($TARGET_URL);
print Dumper($res);

1;

