#!/usr/bin/perl

use strict;
use warnings;
use Test::More;
use Module::Find;
use ex::lib qw(../lib);

setmoduledirs( $INC[0] );
my @modules = grep { !/^$/ } findallmod 'Queue';
plan tests => scalar( @modules );
use_ok ($_) foreach @modules;

diag( "Testing Queue::PQ $Queue::PQ::VERSION, Perl $], $^X" );
