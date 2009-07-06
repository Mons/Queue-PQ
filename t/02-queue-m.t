#!/usr/bin/env perl -w

use strict;
use Test::More qw(no_plan);
use ex::lib qw(../lib);
use Queue::PQ;
use R::Dump;
use Time::HiRes qw(sleep);

my $q=  Queue::PQ->new();
my $dst = 'test';
my $pri = 3;

my %t;
for (1..3) {
	$t{$_} = $q->put(0,test => $pri,$_,0);
	is $t{$_}, $_, 'ins '.$_;
}

my $taken;

$taken = $q->take(0,'test',  );
is $taken->{id}, 1, '1. taken 1=1';
is $taken->{data}, 1;
$q->release(0, $dst => $taken->{id});
undef $taken;

$taken = $q->take(0,'test',  );
is $taken->{id}, 1, '2. taken 1=1';
is $taken->{data}, 1;
$q->release(0,$dst => $taken->{id});
undef $taken;

ok $q->update(0,'test',   1, $pri, 'up', 0 );

$taken = $q->take(0,'test',  );
is $taken->{id}, 1, '3. taken 1=1';
is $taken->{data}, 'up';
$q->release(0,'test',  $taken->{id});
undef $taken;

ok $q->update(0,'test',   1, $pri+1, 'up2', 0 );

my @rel;

my $tk;
$tk = $q->take(0,'test',  );push @rel, $tk->{id};
$tk = $q->take(0,'test',  );push @rel, $tk->{id};

is $tk->{id}, 3;
is $tk->{data}, 3;

$taken = $q->take(0,'test',  );
is $taken->{pri}, $pri+1 or diag Dump $taken;
is $taken->{id}, 1 or diag Dump $taken;
is $taken->{data}, 'up2' or diag Dump $taken;

$q->release(0,'test',  $taken->{id});
$q->release(0,'test',  $_) for reverse @rel;

for (2..3) {
	my $t = $q->take(0,'test',  );
	is $t->{id},$_ ,'take '.$_;
	ok $q->ack(0,'test',  $t->{id}), 'ack '.$_;
}

is $q->push(0,'test',  $pri+1,4,0), 4;

$taken = $q->take(0,'test',  );
is $taken->{pri}, $pri+1 or diag Dump $taken;
is $taken->{id}, 4 or diag Dump $taken;
is $taken->{data}, '4' or diag Dump $taken;

$taken = $q->take(0,'test',  );
is $taken->{pri}, $pri+1 or diag Dump $taken;
is $taken->{id}, 1 or diag Dump $taken;
is $taken->{data}, 'up2' or diag Dump $taken;

ok $q->release(0,'test',  $_) for 1,4;

#$q->delete(0,'test',  1);
#diag Dump $q;
diag Dump $q->fullstats;

ok $q->delete(0,'test',  $_) for 1,4;

undef $taken;

is $q->put(0,$dst => $pri, 4, 0.5, 55), 55;
is $q->put(0,$dst => $pri, 4, 0.2, 99), 99;
is $q->put(0,$dst => $pri, 4, 0.1, 77), 77;
is $q->put(0,$dst => $pri, 4, 0.3, 33), 33;

is $q->put(0,$dst => $pri, 4, 50, 999), 999;

is $q->stats(0,$dst)->{delayed}, 5;

#diag Dump ($q->take(0,$dst));

is scalar $q->take(0,$dst), undef;
sleep 0.11;
my $d1 = $q->take(0,$dst);
ok $d1, 'taken delayed 1';
is $d1->{id}, 77;
sleep 0.1;
my $d2 = $q->take(0,$dst);
ok $d2, 'taken delayed 2';
is $d2->{id}, 99;
sleep 0.1;
my $d3 = $q->take(0,$dst);
ok $d3, 'taken delayed 3';
is $d3->{id}, 33;
sleep 0.2;
my $d4 = $q->take(0,$dst);
ok $d4, 'taken delayed 4';
is $d4->{id}, 55;

is $q->stats->{delayed}, 1;

ok $q->ack(0,$dst => $_), "ack $_" for 55,99;

ok $q->bury(0,$dst => 33),'buried';
ok $q->bury(0,$dst => 77),'buried';

is scalar $q->take(0,$dst), undef, 'buried not takeable';

is $q->stats(0,$dst)->{buried}, 2;

ok $q->delete(0,$dst => 77),'delete buried';

is $q->stats(0,$dst)->{buried}, 1;

ok $q->delete(0,$dst => 999),'delete delayed';

is $q->stats(0,$dst)->{delayed}, 0;

ok $q->delete(0,$dst => 33),'delete buried';

is $q->stats(0,$dst)->{buried}, undef; # since queue is empty, it's dropped

#print Dump [ $q ];

#print Dump + $taken;
