#include <gtest/gtest.h>
#include "nvmefs.hpp"
#include "nvmefs_config.hpp"
#include "utils/gtest_utils.hpp"
#include "utils/fake_device.hpp"

namespace duckdb {

class NoDiskInteractionTest : public testing::Test {
protected:
	NoDiskInteractionTest() {
		// Set up the test environment
		file_system = make_uniq<NvmeFileSystem>(gtestutils::TEST_CONFIG, make_uniq<FakeDevice>(0));
	}

	unique_ptr<NvmeFileSystem> file_system;
};

class DiskInteractionTest : public testing::Test {

protected:
	DiskInteractionTest() {
		// Set up the test environment
		idx_t block_size = 4096;
		idx_t page_size = 4096 * 64;
		idx_t lba_count = (1ULL << 30) / block_size; // 1 GiB

		NvmeConfig testConfig {
		    .device_path = "/dev/ng1n1",
		    .plhdls = 8,
		    .max_temp_size = page_size * 10, // 10 pages = 640 blocks
		    .max_wal_size = 1ULL << 25       // 32 MiB
		};

		file_system = make_uniq<NvmeFileSystem>(testConfig, make_uniq<FakeDevice>(lba_count)); // 1 GiB
	}

	uint64_t end_of_db_lba;
	unique_ptr<NvmeFileSystem> file_system;
};

TEST_F(NoDiskInteractionTest, GetNameReturnsName) {
	string result = file_system->GetName();

	EXPECT_EQ(result, "NvmeFileSystem");
}

TEST_F(NoDiskInteractionTest, CanHandleFileValidPathReturnsTrue) {
	bool result = file_system->CanHandleFile("nvmefs://test.db");
	EXPECT_TRUE(result);
}

TEST_F(NoDiskInteractionTest, CanHandleFileInvalidPathReturnsFalse) {
	bool result = file_system->CanHandleFile("test.db");
	EXPECT_FALSE(result);
}

///// With disk interactions

TEST_F(DiskInteractionTest, FileSyncDoesNothingAsExpected) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	file_system->FileSync(*fh);
}

TEST_F(DiskInteractionTest, OnDiskFileReturnsTrue) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	EXPECT_TRUE(file_system->OnDiskFile(*fh));
}

TEST_F(DiskInteractionTest, FileExistsNoMetadataReturnFalse) {
	bool result = file_system->FileExists("nvmefs://test.db");
	EXPECT_FALSE(result);
}

TEST_F(DiskInteractionTest, FileExistsConfirmsDatabaseExists) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	// Ensure that there is data in the database
	vector<char> hello_buf {'H', 'E', 'L', 'L', 'O'};
	fh->Write(hello_buf.data(), hello_buf.size());

	bool exists = file_system->FileExists("nvmefs://test.db");
	EXPECT_TRUE(exists);
}

/*
* TODO: Figure out if we need the if check in fileexists database case, and why.
* Right now we have fallthrough, but the location of start and location will be the same for WAL before any
* writes. Hence, this will always be false. We should include a check for WAL or something or
* completely handle it in its own case.
TEST_F(DiskInteractionTest, FileExistGivenValidWALFileReturnsTrue) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	vector<char> hello_buf {'H', 'E', 'L', 'L', 'O'};
	fh->Write(hello_buf.data(), hello_buf.size());

	bool exists = file_system->FileExists("nvmefs://test.db.wal");
	EXPECT_TRUE(exists);
}
*/

TEST_F(DiskInteractionTest, FileExistsThrowsIOExceptionIfMultipleDatabases) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	EXPECT_THROW(file_system->FileExists("nvmefs://xyz.db"), IOException);
}

TEST_F(DiskInteractionTest, FileExistsReturnFalseWhenTemporaryFileDoNotExists) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	bool exists = file_system->FileExists("nvmefs:///tmp/file");
	EXPECT_FALSE(exists);
}

TEST_F(DiskInteractionTest, FileExistsReturnTrueWhenTemporaryFileExists) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	// Write something to create file
	string hello = "hello temp";
	vector<char> hello_buf {hello.begin(), hello.end()};
	int bytes_to_read_write = hello.size();
	fh = file_system->OpenFile("nvmefs:///tmp/file", flags);
	fh->Write(hello_buf.data(), bytes_to_read_write);

	// Check if it exists
	bool exists = file_system->FileExists("nvmefs:///tmp/file");
	EXPECT_TRUE(exists);

	// Read back data
	vector<char> buffer(bytes_to_read_write);
	fh->Read(buffer.data(), bytes_to_read_write, 0);

	EXPECT_EQ(string(buffer.begin(), buffer.end()), hello);
}

TEST_F(DiskInteractionTest, OpenFileCompleteInvalidPathThrowInvalidInputException) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_WRITE;
	ASSERT_THROW(file_system->OpenFile("nvmefs://test", flags), InvalidInputException);
}

TEST_F(DiskInteractionTest, OpenFileInvalidDBPathThrowIOException) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_WRITE;
	ASSERT_THROW(file_system->OpenFile("nvmefs://test.wal", flags), IOException);
}

TEST_F(DiskInteractionTest, OpenFileValidDBPathThrowNoExpeception) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_WRITE;
	ASSERT_NO_THROW(file_system->OpenFile("nvmefs://test.db", flags));
}

TEST_F(DiskInteractionTest, OpenFileProducesCorrectFileHandle){
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);
	NvmeFileHandle& nvme_fh = fh->Cast<NvmeFileHandle>();
	NvmeFileSystem& nvme_fs = nvme_fh.file_system.Cast<NvmeFileSystem>();


	EXPECT_EQ(nvme_fh.path, "nvmefs://test.db");
	EXPECT_EQ(nvme_fh.flags.OpenForWriting(), true);

	// Check that underlying filesytem is the same
	EXPECT_EQ(&nvme_fs, &*file_system);
}

TEST_F(DiskInteractionTest, WriteAndReadData) {

	// Create a file
	string file_path = "nvmefs://test.db";
	unique_ptr<FileHandle> file =
	    file_system->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	ASSERT_TRUE(file != nullptr);

	// Write some data to the file
	char *data_ptr = "Hello, World!";
	int data_size = strlen(data_ptr);
	file->Write(data_ptr, data_size, 0);

	// Read the data back
	vector<char> buffer(data_size);
	file->Read(buffer.data(), data_size, 0);

	// Check that the data is correct
	EXPECT_EQ(string(buffer.data(), data_size), data_ptr);
}

TEST_F(DiskInteractionTest, WriteAndReadDataDoesNotOverlapOtherCategories) {

	// Create a file
	string file_path = "nvmefs://test.db";
	string wal_file_path = "nvmefs://test.db.wal";
	string tmp_file_path =
	    StringUtil::Format("nvmefs://test.db/tmp/duckdb_temp_storage_%d-%llu.tmp", DEFAULT_BLOCK_ALLOC_SIZE, 0);
	unique_ptr<FileHandle> db_file =
	    file_system->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	ASSERT_TRUE(db_file != nullptr);

	unique_ptr<FileHandle> wal_file =
	    file_system->OpenFile(wal_file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	ASSERT_TRUE(wal_file != nullptr);

	unique_ptr<FileHandle> tmp_file =
	    file_system->OpenFile(tmp_file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	ASSERT_TRUE(tmp_file != nullptr);

	// Write some data to the db file
	char *db_data_ptr = "Hello, db!";
	int db_data_size = strlen(db_data_ptr);
	db_file->Write(db_data_ptr, db_data_size, 0);

	char *wal_data_ptr = "Hello, wal!";
	int wal_data_size = strlen(wal_data_ptr);
	wal_file->Write(wal_data_ptr, wal_data_size, 0);

	char *tmp_data_ptr = "Hello, tmp!";
	int tmp_data_size = strlen(tmp_data_ptr);
	tmp_file->Write(tmp_data_ptr, tmp_data_size, 0);

	// Read the data back
	vector<char> db_buffer(db_data_size);
	db_file->Read(db_buffer.data(), db_data_size, 0);

	vector<char> wal_buffer(wal_data_size);
	wal_file->Read(wal_buffer.data(), wal_data_size, 0);

	vector<char> tmp_buffer(tmp_data_size);
	tmp_file->Read(tmp_buffer.data(), tmp_data_size, 0);

	// Check that the data is correct
	EXPECT_EQ(string(db_buffer.data(), db_data_size), db_data_ptr);
	EXPECT_EQ(string(wal_buffer.data(), wal_data_size), wal_data_ptr);
	EXPECT_EQ(string(tmp_buffer.data(), tmp_data_size), tmp_data_ptr);
}

// TODO: Make this parameterized to test different byte offsets whithin different blocks
TEST_F(DiskInteractionTest, WriteAndReadDataWithinBlock) {

	// Create a file
	string file_path = "nvmefs://test.db";
	unique_ptr<FileHandle> file =
	    file_system->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	ASSERT_TRUE(file != nullptr);

	// Write some data to the file
	char *data_ptr = "Hello, World!";
	int data_size = strlen(data_ptr);
	file->Write(data_ptr, data_size, 16); // Write data at the 16th byte of the device

	// Read the data back
	vector<char> buffer(data_size);
	file->Read(buffer.data(), data_size, 16); // Read data from the 16th byte of the device

	// Check that the data is correct
	EXPECT_EQ(string(buffer.data(), data_size), data_ptr);
}

TEST_F(DiskInteractionTest, WriteAndReadDataWithSeek) {

	// Create a file
	string file_path = "nvmefs://test.db";
	unique_ptr<FileHandle> file =
	    file_system->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	ASSERT_TRUE(file != nullptr);

	idx_t block_location = 4096 * 5; // 5 blocks of 4096 bytes each

	// Write some data to the file
	char *data_ptr = "Hello, World!";
	int data_size = strlen(data_ptr);
	file->Write(data_ptr, data_size, block_location);

	// Seek to the beginning of the file
	file->Seek(4096 * 3);

	// The old 5th block should now be translated to the 0th block after seek
	vector<char> buffer(data_size);
	file->Read(buffer.data(), data_size, 4096 * 2);

	// Check that the data is correct
	EXPECT_EQ(string(buffer.data(), data_size), data_ptr);
}

TEST_F(DiskInteractionTest, SeekOutOfBounds) {
	// Create a file
	string file_path = "nvmefs://test.db";
	unique_ptr<FileHandle> file =
	    file_system->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	ASSERT_TRUE(file != nullptr);

	// Attempt to seek out of bounds
	EXPECT_THROW(file->Seek((1ULL << 31) + 1), std::runtime_error);
}

TEST_F(DiskInteractionTest, ReadAndWriteReturningNumberOfBytes) {
	string file_path = "nvmefs://test.db";
	unique_ptr<FileHandle> file =
	    file_system->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	ASSERT_TRUE(file != nullptr);

	// Write some data to the file
	char *data_ptr = "Hello, World!";
	uint64_t bytes_written = file->Write(data_ptr, 13);

	// Read the data
	vector<char> buffer(13);
	uint64_t bytes_read = file->Read(buffer.data(), 13);

	// Check that the number of bytes written and read is correct
	EXPECT_EQ(bytes_written, 13);
	EXPECT_EQ(bytes_read, 13);
	EXPECT_EQ(string(buffer.data(), bytes_read), data_ptr);
}

TEST_F(DiskInteractionTest, ReadWithReturnIOfBytesAfterSettingSeek) {
	string file_path = "nvmefs://test.db";
	unique_ptr<FileHandle> file =
	    file_system->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	ASSERT_TRUE(file != nullptr);

	// Write some data to the file
	char *data_ptr = "Hello, World!";
	file->Write(data_ptr, 13, 4096 * 64);

	// Move file pointer to next duckdb page
	file->Seek(4096 * 64);

	// Read the data
	vector<char> buffer(13);
	uint64_t bytes_read = file->Read(buffer.data(), 13);

	// Check that the number of bytes written and read is correct
	EXPECT_EQ(bytes_read, 13);
	EXPECT_EQ(string(buffer.data(), bytes_read), data_ptr);
}

// TODO: Think about how this should be handled when the file system defines the ranges of LBA's
// TEST_F(DiskInteractionTest, WriteOutOfRange) {
// 	// Create a file
// 	string file_path = "nvmefs://test.db";
// 	unique_ptr<FileHandle> file =
// 	    file_system->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
// 	ASSERT_TRUE(file != nullptr);

// 	// Attempt to write data out of range
// 	char *data_ptr = "Hello, World!";
// 	int data_size = strlen(data_ptr);
// 	EXPECT_THROW(file->Write(data_ptr, data_size, (1ULL << 30) + 1), std::runtime_error);
// }

} // namespace duckdb
