#include "rados_io.hpp"

#include <stdexcept>

using std::runtime_error;

#define MIN(a, b) ((a) < (b) ? (a) : (b))

rados_io::rados_io(const conn_info &ci, string pool)
{
	int ret;

	if ((ret = cluster.init2(ci.user.c_str(), ci.cluster.c_str(), ci.flags)) < 0) {
		throw runtime_error("rados_io::rados_io() failed "
				"(couldn't initialize the cluster handle)");
	}

	if ((ret = cluster.conf_read_file("/home/ubuntu/develop/ceph/build/ceph.conf")) < 0) {
		cluster.shutdown();
		throw runtime_error("rados_io::rados_io() failed "
				"(couldn't read the Ceph configuration file)");
	}

	if ((ret = cluster.connect()) < 0) {
		cluster.shutdown();
		throw runtime_error("rados_io::rados_io() failed "
				"(couldn't connect to cluster)");
	}


	if ((ret = cluster.ioctx_create(pool.c_str(), ioctx)) < 0) {
		cluster.shutdown();
		throw runtime_error("rados_io::rados_io() failed "
				"(couldn't set up ioctx)");
	}
}

rados_io::~rados_io(void)
{
	ioctx.close();

	cluster.shutdown();
}

size_t rados_io::read(const string &key, char *value, off_t offset, size_t len)
{
	int ret;

	librados::bufferlist bl = librados::bufferlist::static_from_mem(value, len);

	ret = ioctx.read(key, bl, len, offset);
	if (ret >= 0) {
  }
  else {
		throw runtime_error("rados_io::read() failed");
	}

  // TODO:
  // remove memcpy 
	memcpy(value, bl.c_str(), len);
	
	return ret;
}

size_t rados_io::write(const string &key, const char *value, off_t offset, size_t len)
{
	int ret;

	std::cout << std::string(value) << std::endl;
	librados::bufferlist bl = librados::bufferlist::static_from_mem(const_cast<char *>(value), len);
	ret = ioctx.write(key, bl, len, offset);

	if (ret >= 0) {
		return len;
	} else {
		throw runtime_error("rados_io::write() failed");
	}

}

size_t rados_io::read_large(const string &key, char *value, off_t offset, size_t len)
{
	off_t cursor = offset;
	off_t stop = offset + len;
	size_t ret = 0;

	while (cursor < stop) {
		uint64_t obj_num = cursor >> OBJ_BITS;

		off_t next_bound = (cursor & OBJ_MASK) + OBJ_SIZE;
		size_t sub_len = MIN(next_bound - cursor, stop - cursor);

		ret += this->read(key + "$" + std::to_string(obj_num),
				value + ret,
				cursor & (~OBJ_MASK),
				sub_len);

		cursor = next_bound;
	}

	return ret;
}

size_t rados_io::write_large(const string &key, const char *value, off_t offset, size_t len)
{
	off_t cursor = offset;
	off_t stop = offset + len;
	size_t ret = 0;

	while (cursor < stop) {
		uint64_t obj_num = cursor >> OBJ_BITS;

		off_t next_bound = (cursor & OBJ_MASK) + OBJ_SIZE;
		size_t sub_len = MIN(next_bound - cursor, stop - cursor);

		ret += this->write(key + "$" + std::to_string(obj_num),
				value + ret,
				cursor & (~OBJ_MASK),
				sub_len);

		cursor = next_bound;
	}

	return ret;
}

size_t rados_io::read(const string &key, string& value, off_t offset, size_t len)
{
	uint64_t size;
	time_t mtime;
	int ret;

	if (!len) {
		if ((ret = ioctx.stat(key, &size, &mtime)) < 0) {
				throw runtime_error("rados_io::read() failed");
			}
		}

		len = (offset < size) ? (size - offset) : 0;

	value.resize(len);
	librados::bufferlist bl = librados::bufferlist::static_from_string(value);

	ret = ioctx.read(key, bl, len, offset);
	if (ret >= 0) {
		throw runtime_error("rados_io::read() failed");
	}

	value = bl.to_str();
	return ret;
}

size_t rados_io::write(const string &key, const string &value, off_t offset)
{
	int ret;

	librados::bufferlist bl = librados::bufferlist::static_from_string
			(const_cast<string &>(value));

	if (offset) {
		ret = ioctx.write(key, bl, value.length(), offset);
	} else {
		ret = ioctx.write_full(key, bl);
	}

	if (ret >= 0) {
	} else {
		throw runtime_error("rados_io::write() failed");
	}

	return value.length();
}

bool rados_io::exist(const string &key)
{
	int ret;
	uint64_t size;
	time_t mtime;

	ret = ioctx.stat(key, &size, &mtime);
	if (ret >= 0) {
		return true;
	} else if (ret == -ENOENT) {
		return false;
	} else {
		throw runtime_error("rados_io::exist() failed");
	}
}

void rados_io::remove(const string &key)
{
	int ret;

	ret = ioctx.remove(key);
	if (ret >= 0) {
	} else if (ret == -ENOENT) {
	} else {
		throw runtime_error("rados_io::remove() failed");
	}
}

size_t rados_io::read_bench(size_t num_op, std::map<std::string, clock_t>& lrtime, std::map<std::string, clock_t>& rltime, size_t io_size) {
  size_t len = 0; 
  ioctx.new_times();
  for (size_t i = 0; i < num_op; i++) {
    len += read("obj" + std::to_string(i), read_buffer, 0, sizeof(*read_buffer)*io_size);
  }
  ioctx.get_times(lrtime, rltime);
  ioctx.delete_times();
  return len;
}

size_t rados_io::write_bench(size_t num_op, std::map<std::string, clock_t>& lrtime, std::map<std::string, clock_t>& rltime, size_t io_size) {
  size_t len = 0;
  ioctx.new_times();
  for (size_t i = 0; i < num_op; i++) {
    len += write("obj" + std::to_string(i), write_buffer, 0, sizeof(*write_buffer)*io_size); 
  }
  ioctx.get_times(lrtime, rltime);
  ioctx.delete_times();
  return len;
}

void rados_io::prepare_bench(size_t io_size) {
  read_buffer = new char[io_size];
  write_buffer = (char*)generate_random_bytes(io_size);
}

void rados_io::close_bench() {
  delete[] read_buffer;
  delete[] write_buffer;
}

unsigned char *rados_io::generate_random_bytes(size_t size) {
  unsigned char *random_bytes = new unsigned char[size];
  srand((unsigned int) time(NULL));
  for (size_t i = 0; i < size; i++) 
    random_bytes[i] = rand();
  return random_bytes;
}

