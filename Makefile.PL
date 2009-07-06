use inc::Module::Install;

name     'Queue-PQ';
all_from 'lib/Queue/PQ.pm';
author   'Mons Anderson <mons@cpan.org>';
license  'perl';

build_requires 'Test::More';
build_requires 'Module::Find';
build_requires 'ex::lib';

auto_install;

WriteAll;

