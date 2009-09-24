package Queue::PQ::Single;

use strict;
use Carp;
use Data::Dumper;
use Time::HiRes qw(time);

our %ZERO_STATS = (
	prios    => [],
	map { $_ => 0 } qw( urgent ready delayed buried taken active total ),
);

sub nextval {
	my $self = shift;
	my $id;
	while (!defined $id or exists $self->{j}{$id}) {
		$id = $self->{nextval}->($self);
	}
	return $id;
}

sub new {
	my $pk = shift;
	return bless {
		nextval => sub { ++(shift->{id}) },
		@_,
		j => {}, # job pointer

		q => {}, # ready queue
		d => {}, # delayed queue
		b => {}, # buried queue

		o => {}, # offsets
	}, $pk;
}

=head1 STATS KEYS

	prios
	
	urgent
	ready
	taken
	delayed
	buried

=cut

sub ready_prios {
	my $self= shift;
	return sort { $a <=> $b } map int, keys %{ $self->{q} };
}

sub delayed_prios {
	my $self= shift;
	return sort { $a <=> $b } map int, keys %{ $self->{d} };
}

sub buried_prios {
	my $self= shift;
	return sort { $a <=> $b } map int, keys %{ $self->{b} };
}

sub prios {
	my $self = shift;
	my %uniq;
	return grep !$uniq{$_}++,$self->ready_prios,$self->delayed_prios, $self->buried_prios;
}

sub stats {
	my $self= shift;
	my $ready   = 0;
	my $urgent  = 0;
	my $delayed = 0;
	my $buried  = 0;
	my $taken   = keys %{ $self->{taken} };
	$self->_collect_delayed(0);
	my @prios = $self->prios;
	for (@prios) {
		$ready   += @{ $self->{q}{$_} || [] };
		$delayed += @{ $self->{d}{$_} || [] };
		$buried  += @{ $self->{b}{$_} || [] };

		$urgent  += @{ $self->{q}{$_} || [] } if $_ < 16;
	}
	my $stats = {
		prios   => \@prios,
		
		urgent  => $urgent,
		ready   => $ready,
		delayed => $delayed,
		buried  => $buried,
		taken   => $taken,
		active  => $ready + $taken + $delayed,
		total   => $ready + $taken + $delayed + $buried,
	};
}

sub empty {
	my $self = shift;
	$self->stats->{total} == 0 ? 1 : 0;
}

sub _add_to_tail {
	my $self = shift;
	my $j    = shift;
	my $pri  = $j->{pri};
	if (!$self->{q}{$pri} or @{$self->{q}{$pri}} == 0) {
		$self->{q}{$pri} = [];
		$self->{o}{$pri} = 0;
	}
	$self->_collect_delayed(0,$pri);
	push @{ $self->{q}{$pri} },$j;
	$j->{'.'} = $#{ $self->{q}{$pri} } - $self->{o}{$pri};
	$j->{state} = 'ready';
	return;
}
sub _add_to_head {
	my $self = shift;
	my $j    = shift;
	my $pri  = $j->{pri};
	if (!$self->{q}{$pri} or @{$self->{q}{$pri}} == 0) {
		$self->{q}{$pri} = [];
		$self->{o}{$pri} = 0;
	}
	unshift @{ $self->{q}{$pri} },$j;
	$self->{o}{$pri}++;
	$j->{'.'} = -$self->{o}{$pri};
	$j->{state} = 'ready';
	return;
}

sub _add_to_delay {
	my $self = shift;
	my $j    = shift;
	my $pri  = $j->{pri};
	$j->{state} = 'delayed';

		my $at = $j->{at};
		my $i = -1;
		if ($self->{d}{$pri}) {
			for($i =0; $i < @{$self->{d}{$pri}}; $i++) {
				last if $at < $self->{d}{$pri}[$i]{at};
			}
		} else {
			$self->{d}{$pri} = [];
		}
		if ($i > -1) {
			# found position
			splice @{ $self->{d}{$pri} },$i,0,$j;
		} else {
			unshift @{ $self->{d}{$pri} },$j;
		}
	
}

sub _add {
	my $self  = shift;
	my $cmd   = shift;     # put or push
	my $pri   = int shift; # priority 0..N
	my $job   = shift;     # job data
	my $delay = shift;     # [ delay, default 0 ]
	my $id    = shift;     # [ predefined job id ]
	if (defined $id) {
		$@ = "DUPLICATE $id" and return if exists $self->{j}{$id};
	} else {
		$id = $self->nextval;
		exists $self->{j}{$id} and die "Internal nextval generated duplicate id: $id";
	}
	#warn "Add: $id / ".(exists $self->{taken}{$id} ? $self->{taken}{$id}{state} : 'not taken');
	
	
	$self->{j}{$id} = {
		id => $id,
		pri => $pri,
		data => $job,
		$delay ? ( at => time+$delay, state => 'delayed' ) : ( state => 'ready' ),
	};
	if ($delay) {
		$self->_add_to_delay($self->{j}{$id});
	} else {
		
		if ($cmd eq 'put') {
			$self->_add_to_tail($self->{j}{$id});
		} elsif ($cmd eq 'push') {
			$self->_add_to_head($self->{j}{$id});
		} else {
			die "Internal error: wrong command $cmd";
		}
	}
	return $id;
}

sub put  : method { shift->_add(put  => @_) }
sub push : method { shift->_add(push => @_) }

sub _collect_taken {
	my $self = shift;
	return;
}

sub _collect_delayed {
	my $self = shift;
	my $count = @_ ? shift : 1;
	my $only = @_ ? shift : undef; # priority
	my $have = 0;
	for my $pri (defined $only ? $only : $self->delayed_prios) {
		next unless exists $self->{d}{$pri};
		my $q = $self->{d}{$pri};
		next if time < $q->[0]{at};
		my $ready = shift @$q;
		delete $self->{d}{$pri} unless @$q;

		unless ($self->{q}{$pri}) {
			$self->{q}{$pri} = [];
			$self->{o}{$pri} = 0;
		}

		delete $ready->{at};
		$self->_add_to_tail($ready);

		last if $count > 0 and ++$have >= $count;
	}
	return;
}

sub _vacuum {
	my $self = shift;
	for my $pri (@_ ? @_ : keys %{ $self->{q} }) {
		if (exists $self->{q}{$pri}) {
			while ( @{ $self->{q}{$pri} } and !defined $self->{q}{$pri} ) {
				shift @{ $self->{q}{$pri} };
				$self->{o}{$pri}--;
			}
			pop @{ $self->{q}{$pri} } while @{ $self->{q}{$pri} } and !defined $self->{q}{$pri}[-1];
			@{ $self->{q}{$pri} } or delete($self->{q}{$pri}), delete ($self->{o}{$pri});
		}
		if (exists $self->{d}{$pri}) {
			delete $self->{d}{$pri} if !@{$self->{d}{$pri}};
		}
		if (exists $self->{b}{$pri}) {
			delete $self->{b}{$pri} if !@{$self->{b}{$pri}};
		}
	}
	return;
}

sub take {
	my $self = shift;
	$self->_collect_taken;
	$self->_collect_delayed(1);
	for my $pri ($self->ready_prios) {
		my $q = $self->{q}{$pri};
		my $taken;
		while (@$q) {
			$taken = shift @$q;
			$self->{o}{$pri}--;
			last if defined $taken;
		}
		#@{ $self->{q}{$pri} } or delete($self->{q}{$pri}), delete ($self->{o}{$pri});
		next unless defined $taken;
		
		$taken->{state} = 'taken';
		#warn "take: $taken->{id}: $taken->{state}";
		delete $taken->{'.'};
		$self->{taken}{$taken->{id}} = $taken;
		return $taken;
	}
	return;
}

sub ack {
	my $self = shift;
	my $id = shift;
	my $j = $self->{j}{$id};
	exists $self->{j}{$id} or warn("Ack failed: Not found $id"),return;
	exists $self->{taken}{$id} or warn("Ack failed: Not taken $id"),return;
	delete $self->{taken}{$id};
	delete $self->{j}{$id};
	if ( $self->{q}{$j->{pri}} and !@{ $self->{q}{$j->{pri}} }) {
		delete $self->{q}{$j->{pri}};
		delete $self->{o}{$j->{pri}};
	}
	return 1;
	#warn Dumper $self->{j}{$id};
}

sub _purge {
	my $self = shift;
	my $id = shift;
	#warn "Purge $id deleted";
	delete $self->{taken}{$id};
	delete $self->{j}{$id};
	return $id;
}

sub release {
	my $self = shift;
	my $id = shift;
	my $delay = shift;
	exists $self->{j}{$id} or warn("Release failed: Not found $id"),return;
	exists $self->{taken}{$id} or warn("Release failed: Not taken $id"),return;
	my $j = $self->{taken}{$id};
	$j->{state} eq 'deleted' and return $self->_purge($id);
	if (defined $delay) {
		$j->{at} = time + $delay;
		$self->_add_to_delay($j);
	} else {
		$self->_add_to_head($j);
	}
	delete $self->{taken}{$id};
	return $id;
}

sub requeue {
	my $self = shift;
	my $id = shift;
	my $delay = shift;
	exists $self->{j}{$id} or warn("Not found $id"),return;
	exists $self->{taken}{$id} or warn("Not taken $id"),return;
	my $j = $self->{taken}{$id};
	$j->{state} eq 'deleted' and return $self->_purge($id);
	if (defined $delay) {
		$j->{at} = time + $delay;
		$self->_add_to_delay($j);
	} else {
		$self->_add_to_tail($j);
	}
	delete $self->{taken}{$id};
	return $id;
}

sub bury {
	my $self = shift;
	my $id = shift;
	exists $self->{j}{$id} or warn("Not found $id"),return;
	exists $self->{taken}{$id} or warn("Not taken $id"),return;
	my $j = delete $self->{taken}{$id};
	$j->{state} eq 'deleted' and return $self->_purge($id);
	$j->{state} = 'buried';
	delete $j->{'.'};
	my $pri = $j->{pri};
	push @{ $self->{b}{$pri}||=[] },$j;
	return $id;
}

sub dig {
	my $self = shift;
	my $pri = shift;
	my $N = shift || 1;
	my $have = @{ $self->{b}{$pri} || [] };
	if ( $N > $have ) {
		$N = $have
	} else {
		$have = $N;
	}
	while ($N--) {
		$self->_add_to_tail(shift @{ $self->{b}{$pri} });
	}
	return $have;
}

sub peek {
	my $self = shift;
	my $id = shift;
	exists $self->{j}{$id} or return;#warn("Not found $id"),return;
	return $self->{j}{$id};
}

sub delete : method {
	my $self = shift;
	my $id = shift;
	exists $self->{j}{$id}
		#or warn("Can't delete: Not found $id"),
		or $@ = "Can't delete: Not found $id" and
		return;
	my $j = $self->{j}{$id};
	if ($j->{state} eq 'deleted') {
		#warn("Can't delete: Already deleted $id");
		return;
	}
	my $pri = $j->{pri};
	if ($j->{state} eq 'ready') {
		use R::Dump;
		warn Dump [ $j,$self->{o}{$pri} ] if !defined $self->{o}{$pri} or !defined $j->{'.'};
		my $idx = $j->{'.'} + $self->{o}{$pri};
		if ( defined $self->{q}{$pri}[ $idx ] and $self->{q}{$pri}[ $idx ] == $j ) {
			$self->{q}{$pri}[ $idx ] = undef;
			#splice @{ $self->{q}{$pri} }, $idx, 1;
			#$self->{o}{$pri}--;
		} else {
			die "Bad index $idx / j.=$j->{'.'}+o.$pri=$self->{o}{$pri} $j <=> $self->{q}{$pri}[ $idx ]";
		}
		delete $self->{j}{$id};
	}
	elsif ( $j->{state} eq 'delayed') {
		# TODO: use offsets
		$self->{d}{$pri} = [ grep $_->{id} != $j->{id}, @{ $self->{d}{$pri} } ];
		delete $self->{j}{$id};
	}
	elsif ( $j->{state} eq 'buried') {
		# TODO: use offsets
		$self->{b}{$pri} = [ grep $_->{id} != $j->{id}, @{ $self->{b}{$pri} } ];
		delete $self->{j}{$id};
	}
	elsif ( $j->{state} eq 'taken' ) {
		#warn "$id: $j->{state} => deleted";
		$j->{state} = 'deleted';
		# Don't purge till return
		#delete $self->{j}{$id};
	}
	else {
		die "Bad job state $j->{state}";
	}
	$self->_vacuum($pri);
	return $id;
}

sub update {
	my $self = shift;
	my $id = shift;

	exists $self->{j}{$id}
		or warn ("Can't update nx id=$id"),
		return
	;

	my $pri  = int shift;
	my $job  = shift;
	
	
	my $j = $self->{j}{$id};
	#if (exists $self->{taken}{$id}) {
	#	warn "Updating taken job $id / $j->{state}";
	#}
	my $old = $self->{j}{$id}{pri};
	if ($old != $pri) {
		if ($j->{state} eq 'taken' or $j->{state} eq 'deleted') {
			#warn "Updating $j->{state} job with piority change";
		} else {
			my $idx = $j->{'.'} + $self->{o}{$old};
			if ( $self->{q}{$old}[ $idx ] == $j ) {
				$self->{q}{$old}[ $idx ] = undef;
				
				$j->{pri} = $pri;
				$self->_add_to_tail($j);
				$self->_vacuum($old);
			} else {
				die "Bad index";
			}
		}
	}
	if ($j->{state} eq 'deleted' ) {
		if (exists $self->{taken}{$id}) {
			$j->{state} = 'taken';
			#warn "$id: deleted => $j->{state}";
		} else {
			die "State deleted but not taken";
		}
	}
	$j->{data} = $job;
	return 1;
}


1;
