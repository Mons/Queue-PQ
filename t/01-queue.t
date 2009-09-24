#!/usr/bin/env perl -w

use strict;
use Test::More qw(no_plan);
use ex::lib qw(../lib);
use Queue::PQ::Single;
use R::Dump;
use Time::HiRes qw(time sleep);

my $q=  Queue::PQ::Single->new();
my $pri = 3;

my %t;
for (1..3) {
	$t{$_} = $q->put($pri,$_,0);
	is $t{$_}, $_, 'ins '.$_;
}

my $taken;

$taken = $q->take;
is $taken->{id}, 1, '1. taken 1=1';
is $taken->{data}, 1;
$q->release($taken->{id});
undef $taken;

$taken = $q->take;
is $taken->{id}, 1, '2. taken 1=1';
is $taken->{data}, 1;
$q->release($taken->{id});
undef $taken;

ok $q->update( 1, $pri, 'up', 0 );

$taken = $q->take;
is $taken->{id}, 1, '3. taken 1=1';
is $taken->{data}, 'up';
$q->release($taken->{id});
undef $taken;

ok $q->update( 1, $pri+1, 'up2', 0 );

my @rel;

my $tk;
$tk = $q->take;push @rel, $tk->{id};
$tk = $q->take;push @rel, $tk->{id};

is $tk->{id}, 3;
is $tk->{data}, 3;

$taken = $q->take;
is $taken->{pri}, $pri+1 or diag Dump $taken;
is $taken->{id}, 1 or diag Dump $taken;
is $taken->{data}, 'up2' or diag Dump $taken;

$q->release($taken->{id});
$q->release($_) for reverse @rel;

for (2..3) {
	my $t = $q->take;
	is $t->{id},$_ ,'take '.$_;
	ok $q->ack($t->{id}), 'ack '.$_;
}

ok $q->push($pri+1,4,0);

$taken = $q->take;
is $taken->{pri}, $pri+1 or diag Dump $taken;
is $taken->{id}, 4 or diag Dump $taken;
is $taken->{data}, '4' or diag Dump $taken;

$taken = $q->take;
is $taken->{pri}, $pri+1 or diag Dump $taken;
is $taken->{id}, 1 or diag Dump $taken;
is $taken->{data}, 'up2' or diag Dump $taken;

$q->ack($_) for 1,4;

undef $taken;

is $q->put($pri, 4, 0.5, 55), 55;
is $q->put($pri, 4, 0.2, 99), 99;
is $q->put($pri, 4, 0.1, 77), 77;
is $q->put($pri, 4, 0.3, 33), 33;

is $q->put($pri, 4, 50, 999), 999;

is $q->stats->{delayed}, 5;

is scalar $q->take, undef;
sleep 0.11;
my $d1 = $q->take;
ok $d1, 'taken delayed 1';
is $d1->{id}, 77;
sleep 0.1;
my $d2 = $q->take;
ok $d2, 'taken delayed 2';
is $d2->{id}, 99;
sleep 0.1;
my $d3 = $q->take;
ok $d3, 'taken delayed 3';
is $d3->{id}, 33;
sleep 0.2;
my $d4 = $q->take;
ok $d4, 'taken delayed 4';
is $d4->{id}, 55;

is $q->stats->{delayed}, 1;

ok $q->ack($_), "ack $_" for 55,99;

ok $q->bury(33),'buried';
ok $q->bury(77),'buried';

is scalar $q->take, undef, 'buried not takeable';

is $q->stats->{buried}, 2, 'buried = 2';

ok $q->delete(77),'delete buried';

is $q->stats->{buried}, 1, 'buried = 1';

ok $q->delete(999),'delete delayed';

is $q->stats->{delayed}, 0, 'delayed = 0';

ok $q->delete(33),'delete buried';

my $stats = $q->stats->{buried};
is $q->stats->{$_}, 0, "$_ = 0" for qw(ready delayed buried taken);

# Empty queue

{
	#Test emptiness
	my $i = 33;
	is $q->put( $pri, 'j11',0, $i ), $i, 'put';
	is $q->stats->{ready}, 1, 'ready=1';
	ok !$q->empty, 'not empty with ready';
	ok $q->take, 'take';
	is $q->stats->{taken}, 1, 'taken=1';
	ok !$q->empty, 'not empty with taken';
	ok $q->delete($i), 'delete';
	is $q->stats->{taken}, 1, 'taken=1';
	ok !$q->empty, 'not empty with taken/deleted';
	ok $q->update($i), 'update/restore';
	is $q->stats->{taken}, 1, 'taken=1';
	ok !$q->empty, 'not empty with taken/restored';
	ok $q->requeue($i), 'requeue';
	is $q->stats->{ready}, 1, 'ready=1';
	ok !$q->empty, 'not empty with ready/requeued';
	ok $q->take, 'take';
	ok $q->bury($i), 'bury';
	is $q->stats->{buried}, 1, 'buried=1';
	ok !$q->empty, 'not empty with buried';
	is $q->dig($pri), 1, 'dig';
	is $q->stats->{ready}, 1, 'ready=1';
	ok !$q->empty, 'not empty with digged';
	ok $q->delete($i), 'delete';
}

# Empty queue
# print Dump [ $q ];exit;

#print Dump [ $q ];

is $q->put( $pri, 'j11',0, 33 ), 33, 'put 33';
is $q->push($pri, 'j11',0, 11 ), 11, 'push 11';
ok $q->delete(33), 'delete 33';
ok $q->delete(11), 'delete 11';

# Empty queue

is $q->push($pri, 'j11',0, 11 ), 11, 'push 11';
is $q->push($pri, 'j22',0, 22 ), 22, 'push 22';
ok $q->delete( $_ ), "delete $_" for 22,11;

is $q->push($pri, 'j11',0, 11 ), 11, 'push 11';
is $q->push($pri, 'j22',0, 22 ), 22, 'push 22';
ok $q->delete( $_ ), "delete $_" for 11,22;

# Empty queue

is $q->push($pri, 'j11',0, 11 ), 11, 'push 11';
is $q->push($pri, 'j22',0, 22 ), 22, 'push 22';
is $q->push($pri, 'j33',0, 33 ), 33, 'push 22';

my $j;
ok $j = $q->take() ,'take';
is $j->{id},33,'taken 33';
ok $q->requeue(33), 'requeue';
ok $q->delete( $_ ), "delete $_" for 33,22,11;


{
	# Test for purge
	my $id = 33;
	for my $action (qw(release requeue bury ack)) {
		my $x = $q->put($pri,'data',0,$id);
		is $x, $id, 'ins '.$id;
		$taken = $q->take;
		is $taken->{id}, $id, 'taken '.$id;
		is $taken->{data}, 'data', 'data ok';
		ok $q->delete($id), 'delete';
		ok !$q->delete($id), 'delete again fail';
		is $taken->{state}, 'deleted', 'queue deletion';
		ok $q->$action($id), $action;
		ok !$q->take, 'no more jobs';
		is $q->stats->{$_}, 0, "$_ = 0" for qw(ready delayed buried taken);
		undef $taken;
	}
	{
		my $x = $q->put($pri,'data',0,$id);
		$q->put($pri,'some trash',0,11);
		$q->put($pri,'some trash',0,22);
		$taken = $q->take;
		#warn Dump $taken;
		is $taken->{id}, $id, 'taken '.$id;
		ok $q->delete($id), 'delete';
		ok !$q->delete($id), 'delete again fail';
		#warn Dump $taken;
		is $taken->{state}, 'deleted', 'queue deletion';
		ok $q->update( $id, $pri+1, 'up', 0 ), 'update ok';
		#warn Dump $taken;
		is $taken->{state}, 'taken', 'deletion cancelled';
		ok $q->update( $id, $pri, 'up', 0 ), 'update ok';
		#warn Dump $taken;
		is $taken->{state}, 'taken', 'deletion cancelled';
		ok $q->update( $id, $pri+1, 'up', 0 ), 'update ok';
		#warn Dump $taken;

=for rem
		ok $q->delete($id), 'delete';
		is $taken->{state}, 'deleted', 'queue deletion';
		ok $q->update( $id, $pri, 'up', 0 ), 'update ok';
		ok $q->release($id), 'released';
		is $taken->{state}, 'ready', 'deletion cancelled';
		$taken = $q->take;
		is $taken->{id}, $id, 'taken '.$id;
		ok $q->delete($id), 'delete';
		ok !$q->delete($id), 'delete again fail';
		ok $q->update( $id, $pri, 'up', 0 ), 'update ok';
=cut

		is $taken->{data}, 'up', 'up data ok';
		ok $q->release($id,0.1), 'release delayed ok';
		my $req = $q->take;
		$q->requeue($req->{id});
		#warn Dump $taken;
		sleep 0.05;
		ok $q->update( $id, $pri, 'upd', 0 ), 'update delayed ok';
		#warn Dump $taken;
		sleep 0.06;
		ok $q->delete($_), "delete $_" for 11,22;
		$taken = $q->take;
		#warn Dump $taken;
		ok $q->requeue($id), 'requeue ok';
		#warn Dump $taken;
		
		is $q->stats->{ready}, 1, 'have 1 ready job';
		ok $q->delete($id), 'delete';
		ok !$q->delete($id), 'delete again fail';
		is $q->stats->{total},0, 'no more jobs';
		undef $taken;
	}
}

#print Dump $q;


__END__
sleep 1;
print Dump [ $q->take ];
#print Dump +$q;
