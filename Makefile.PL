use inc::Module::Install;
require Module::Install::AuthorTests;

name 'IPC-Semaphore-Memcached';
all_from 'lib/IPC/Semaphore/Memcached.pm';

test_requires 'Test::More' => '0.88';
test_requires 'Test::Exception';
test_requires 'Test::Memcached';
author_requires 'Test::NoTabs';
author_requires 'Test::EOL' => '0.7';

requires 'Moose';
requires 'JSON::XS';
requires 'Memcached::libmemcached' => '0.3102';

author_tests 't/author';

resources repository => 'git://github.com/chrisa/ipc-semaphore-memcached.git';

WriteAll;
