package Queue::PQ;

=head1 NAME

Queue::PQ - Implement priority queue, using pluggable engines

=head1 VERSION

Version 0.01

=cut

our $VERSION = '0.01';

use strict;
use warnings;
use Carp;

=head1 SYNOPSIS

    TODO

=cut

use Queue::PQ::Single;
=head1 REFERENCE

	+ put
	+ push

	+ take
	+ requeue
	+ release
	+ ack
	+ bury
	
	+ dig 
	+ peek
	+ delete
	
	+ stats
	+ queues

=cut

my %taken;
my $idx = 1;

sub nextval {
	my $self = shift;
	$self->{nextval}->($self);
}

sub new {
	my $pk = shift;
	
	my $sequence = 0;
	return bless {
		nextval => sub { ++$sequence },
		@_,
		qsize => 0,
		tsize => 0,
		stat => {},
		q => {}, # queues keeper
		c => {}, # clients keeper
		reg => {}, # registered queues
	}, $pk;
}

sub q {
	my $self = shift;
	my $cln = shift;
	my $dst = shift or croak "No destination";
	$self->{q}{$dst} ||= Queue::PQ::Single->new( nextval => $self->{nextval} );
	$self->{c}{$cln}{$dst} ||= {};
	wantarray ? ($self,$self->{q}{$dst},$self->{c}{$cln}{$dst}) : $self->{q}{$dst};
}

sub reset : method {
	my $self = shift;
	my $cln = shift;
	for my $dst ( keys %{ $self->{c}{$cln} } ) {
		for my $id ( keys %{ $self->{c}{$cln}{$dst} } ) {
			$self->{q}{$dst} or next;
			$self->{tsize}--;
			$self->{q}{$dst}->release( $id ) or carp "Release on disconnect $dst.$id failed: $@";
		}
	}

}

sub put {
	my ($self,$q,$c) = &q;
	my ($pri,$data,$delay, $id) = @_;
	$self->{stat}{cmd}{put}++;
	my $r = $q->put($pri, $data, $delay, $id);
	$r and $self->{qsize}++;
	return $r;
}
sub push : method {
	my ($self,$q,$c) = &q;
	my ($pri,$data,$delay,$id) = @_;
	$self->{stat}{cmd}{push}++;
	my $r = $q->push($pri, $data, $delay, $id);
	$r and $self->{qsize}++;
	return $r;
}

sub take {
	my ($self,$q,$c) = &q;
	$self->{stat}{cmd}{take}++;
	if( my $job = $q->take ) {
		$self->{tsize}++;
		$c->{ $job->{id} } = 1;
		return $job;
	} else {
		return undef;
	}
}

sub _give_back {
	my $cmd = shift;
	my ($self,$q,$c) = &q;
	$self->{stat}{cmd}{$cmd}++;
	my ($id,$delay) = @_;
	if ( delete $c->{ $id } ) {
		if($q->$cmd($id,$delay)) {
			$self->{tsize}--;
			return "OK";
		}
		else {
			$@ = "NOT_FOUND"
		}
	} else {
		$@ = "NOT_TAKEN";
	}
	return;
}

sub release { unshift @_, 'release'; goto &_give_back }
sub requeue { unshift @_, 'requeue'; goto &_give_back }
sub ack     { unshift @_, 'ack';     goto &_give_back }
sub bury    { unshift @_, 'bury';    goto &_give_back }

sub peek {
	my ($self,$q,$c) = &q;
	my $id = shift;
	$self->{stat}{cmd}{peek}++;
	if(my $job = $q->peek($id)) {
		return $job;
	}
	else {
		$@ = "NOT_FOUND";
	}
	return;
}

sub delete : method {
	my ($self,$q,$c) = &q;
	my $id = shift;
	$self->{stat}{cmd}{delete}++;
	if($q->delete($id)) {
		$self->{qsize}--;
		return 'OK';
	}
	else {
		$@ = "NOT_FOUND";
	}
	return;
}

sub update {
	my ($self,$q,$c) = &q;
	my ($id,$pri,$data,$delay) = @_;
	$self->{stat}{cmd}{update}++;
	if ( $q->update($id, $pri, $data, $delay) ) {
		return 'OK';
	} else {
		$@ = "NOT_FOUND";
	}
	return;
}

sub dig {
	my ($self,$q,$c) = &q;
	$self->{stat}{cmd}{dig}++;
	return $q->dig(@_);
}

=head1 STATS KEYS

	?q prios
	
	urgent
	ready
	taken
	delayed
	buried
	
	+!q cmd
	+!q queues

=cut

sub _drop_empty {
	my $self = shift;
	for my $dst ( @_ ? @_ : keys %{$self->{q}} ) {
		next unless exists $self->{q}{$dst};
		next if exists $self->{reg}{$dst}; # keep
		delete $self->{q}{$dst} if $self->{q}{$dst}->empty;
	}
}

sub stats {
	my $self = shift;
	my $c = shift;
	$self->{stat}{cmd}{stats}++;
	$self->_drop_empty(@_);
	if (@_) {
		my $dst = shift;
		return {} if !exists $self->{q}{$dst};
		return $self->{q}{$dst}->stats;
	}
	
	my $stats;
	for my $dst ( keys %{$self->{q}} ) {
		my $qst = $self->{q}{$dst}->stats;
		#warn "$dst = ".Dumper $qst;
		for ( keys %$qst ) {
			if ( ref $qst->{$_} ) {
				# dont collect ref info
				#$stats->{$_}{$dst} = $qst->{$_};
			} else {
				$stats->{$_} += $qst->{$_} 
			}
		};
		
	}
	$stats = { %Queue::PQ::Single::ZERO_STATS } unless $stats;
	$stats->{cmd} = $self->{stat}{cmd};
	$stats->{queues} = [ $self->queues ];
	
	return $stats;
}

sub fullstats {
	my $self = shift;
	my $c = shift;
	$self->{stat}{cmd}{fullstats}++;
	return $self->($c,@_) if @_;
	$self->_drop_empty(@_);

	use Tie::IxHash;

	my %stats;
	#tie my %stats, 'Tie::IxHash';
	$stats{queues} = [ $self->queues ];
	#tie %{$stats{queue}}, 'Tie::IxHash';
	$stats{queue} = {};
	for my $dst ( @{$stats{queues}} ) {
		$stats{queue}{$dst} = $self->{q}{$dst}->stats;
	}
	$stats{cmd} = $self->{stat}{cmd};
	
	return \%stats;
}


sub create {
	my $self = shift;
	my $cln = shift;
	$self->{stat}{cmd}{create}++;
	my $dst = shift or croak "No destination";
	$self->_drop_empty($dst);
	if (exists $self->{reg}{$dst}) {
		return 'OK EXISTS';
	}
	else {
		$self->{reg}{$dst} = 1;
		$self->{q}{$dst} ||= Queue::PQ::Single->new( nextval => $self->{nextval} );
		return 'OK CREATED';
	}
}

sub drop {
	my $self = shift;
	my $cln = shift;
	$self->{stat}{cmd}{drop}++;
	my $dst = shift or croak "No destination";
	unless (exists $self->{q}{$dst}) {
		return 'NOT EXISTS';
	}
	else {
		delete $self->{reg}{$dst};
		my $count = $self->{q}{$dst}->stats->{active};
		delete $self->{q}{$dst};
		return 'OK '.$count;
	}
}

sub queues {
	my $self = shift;
	return sort keys %{$self->{q}};
}

=head1 AUTHOR

Mons Anderson, C<< <mons at cpan.org> >>

=head1 COPYRIGHT & LICENSE

Copyright 2009 Mons Anderson, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

=cut

1; # End of Queue::PQ
