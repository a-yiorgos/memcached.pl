I was working on some stuff with MIMEDefang, Cache::Memcached and memcached at $work and stumbled upon just that. I wanted to check what exactly was going on while developing. About two hours after reading the informal text protocol specification for memcached, I had a crude working implementation of set and get in Perl and keys stored in a BerkeleyDB hash so that they could be inspected by external tools like makemap and postmap.

I’ve cut a lot of corners in this implementation, like:

- the delete queues are not implemented (yet)
- no check is done whether the inserted value is of the declared length in bytes
- an inserted value cannot contain a \n
- It is not demonizing yet

http://blog.postmaster.gr/2012/03/20/memcached-pl-an-incomplete-implementation-in-perl-with-persistence/
