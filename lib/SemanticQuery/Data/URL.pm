package SemanticQuery::Data::URL;

use Carp;
use Mouse;

# attributes
has 'TARGET_URL' => (is => 'ro', isa => 'Str', required => 1);
has 'API_TOKEN'  => (is => 'rw', isa => 'Str', required => 0); # deferred attribute
has 'MONGO_PTR'  => (is => 'rw', isa => 'MongoDB::Database', required => 0); # deferred attribute
has 'RESULTS'    => (is => 'rw', isa => 'HashRef', required => 0);

use strict;
use warnings;
use SemanticQuery::Logger;

sub process {
	# lazy package loading
	require SemanticQuery::TextWise::API::Category;
	require SemanticQuery::TextWise::API::Concept;
	require MongoDB;

	my $self = shift;
	unless (defined($self->API_TOKEN)) {
		carp(__PACKAGE__ . ' requires API token');
		return;
	}
	unless (defined($self->MONGO_PTR)) {
		carp(__PACKAGE__ . 'requires MongoDB connection object');
		return;
	}

	my $categorizer = SemanticQuery::TextWise::API::Category->new(API_TOKEN => $self->API_TOKEN);
	my $conceiver = SemanticQuery::TextWise::API::Concept->new(API_TOKEN => $self->API_TOKEN);

	my $categories = $categorizer->process($self->TARGET_URL);
	my $concepts = $conceiver->process($self->TARGET_URL);

	my $doc_id = $self->MONGO_PTR->get_collection('entries')->insert({
		url => $self->TARGET_URL,
		categories => $categories,
		concepts => $concepts
	});

	my $ret = {
		status => "OK",
		id => $doc_id->to_string
	};
	return $ret;
}

1;
