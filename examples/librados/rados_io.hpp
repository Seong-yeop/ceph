#ifndef _RADOS_IO_HPP_
#define _RADOS_IO_HPP_

#include <string>
#include <iostream>
#include <rados/librados.hpp>
#include <chrono>

using std::string;

#define METADATA_POOL	"nmfs_metadata"
#define DATA_POOL	"nmfs_data"

#define OBJ_SIZE	(4194304) // == 4MB
#define OBJ_BITS	(22)
#define OBJ_MASK	((~0) << (OBJ_BITS))

class rados_io {
private:
	librados::Rados cluster;
	librados::IoCtx ioctx;
  char *read_buffer;
  char *write_buffer;


public:
	struct conn_info {
		string user;
		string cluster;
		int64_t flags;
	};

	rados_io(const conn_info &ci, string pool);
	~rados_io(void);


	/*
	 * read/write for char* type value (for data operation)
	 */
  size_t read(const string &key, char *value, off_t offset, size_t len);
	size_t write(const string &key, const char *value, off_t offset, size_t len);

	size_t read_large(const string &key, char *value, off_t offset, size_t len);
	size_t write_large(const string &key, const char *value, off_t offset, size_t len);

	/*
	 * synchronous operations
	 *
	 * When len == 0, read() function reads the whole value starting from offset.
	 */

	/*
	 * read/write for string type value
	 */
	size_t read(const string &key, string &value, off_t offset = 0, size_t len = 0);
	size_t write(const string &key, const string &value, off_t offset = 0);
	bool exist(const string &key);
	void remove(const string &key);

  /*
   * bench for measuring latency
   */
  void prepare_bench(size_t io_size);
  void close_bench();
  size_t read_bench(size_t num_op, std::unordered_map<std::string, clock_t>& lrtime, 
      std::unordered_map<std::string, clock_t>& rltime, size_t io_size);
  size_t write_bench(size_t num_op, std::unordered_map<std::string, clock_t>& lrtime,
      std::unordered_map<std::string, clock_t>& rltime, size_t io_size);
  unsigned char *generate_random_bytes(size_t size);
};

#endif /* _RADOS_IO_HPP_ */
