#include <gtest/gtest.h>
#include "nvmefs.hpp"
#include "nvmefs_config.hpp"
#include "nvmefs_temporary_block_manager.hpp"
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

TEST_F(DiskInteractionTest, FileExistGivenValidWALFileReturnsTrue) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	vector<char> hello_buf {'H', 'E', 'L', 'L', 'O'};
	fh->Write(hello_buf.data(), hello_buf.size());

	bool exists = file_system->FileExists("nvmefs://test.db.wal");
	EXPECT_TRUE(exists);
}

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

TEST_F(DiskInteractionTest, GetFileSizeDbWithThreeBlocksReturnsThreeBlocksInBytes) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	DeviceGeometry geo = file_system->GetDevice().GetDeviceGeometry();

	// Read and Write three blocks
	vector<char> buf_h {'H', 'E', 'L', 'L', 'O'};
	vector<char> buf_w {'W', 'O', 'R', 'L', 'D'};
	vector<char> buf_s {'S', 'M', 'I', 'L', 'E'};

	fh->Write(buf_h.data(), buf_h.size(), geo.lba_size * 0);
	fh->Write(buf_w.data(), buf_w.size(), geo.lba_size * 1);
	fh->Write(buf_s.data(), buf_s.size(), geo.lba_size * 2);

	vector<char> res_h(buf_h.size());
	vector<char> res_w(buf_w.size());
	vector<char> res_s(buf_s.size());

	fh->Read(res_h.data(), buf_h.size(), geo.lba_size * 0);
	fh->Read(res_w.data(), buf_w.size(), geo.lba_size * 1);
	fh->Read(res_s.data(), buf_s.size(), geo.lba_size * 2);

	EXPECT_EQ(res_h, buf_h);
	EXPECT_EQ(res_w, buf_w);
	EXPECT_EQ(res_s, buf_s);

	// Ensure correct file size of database
	int64_t expected = geo.lba_size * 3;
	int64_t result = file_system->GetFileSize(*fh);

	EXPECT_EQ(result, expected);
}

TEST_F(DiskInteractionTest, GetFileSizeWALWithNoEntriesReturnZeroBytes) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);
	fh = file_system->OpenFile("nvmefs://test.db.wal", flags);

	EXPECT_EQ(file_system->GetFileSize(*fh), 0);
}

TEST_F(DiskInteractionTest, GetFileSizeOpensTwoTemporaryFileReturnCorrectSizes) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);
	DeviceGeometry geo = file_system->GetDevice().GetDeviceGeometry();

	// Open two temporary files, write to the second
	unique_ptr<FileHandle> tmp_fh_1 = file_system->OpenFile("nvmefs:///tmp/file1", flags);
	unique_ptr<FileHandle> tmp_fh_2 = file_system->OpenFile("nvmefs:///tmp/file2", flags);

	vector<char> buf_h {'H', 'E', 'L', 'L', 'O'};
	tmp_fh_2->Write(buf_h.data(), buf_h.size(), geo.lba_size * 0);
	vector<char> res_h(buf_h.size());
	tmp_fh_2->Read(res_h.data(), buf_h.size(), geo.lba_size * 0);

	EXPECT_EQ(res_h, buf_h);

	// Check file sizes
	EXPECT_EQ(file_system->GetFileSize(*tmp_fh_1), 0);
	EXPECT_EQ(file_system->GetFileSize(*tmp_fh_2), geo.lba_size * 1);
}

TEST_F(DiskInteractionTest, DirectoryExistsNoLoadedMetadataReturnsFalse) {
	EXPECT_FALSE(file_system->DirectoryExists("nvmefs:///tmp"));
}

TEST_F(DiskInteractionTest, DirectoryExistsMetadataLoadedReturnsTrue) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	EXPECT_TRUE(file_system->DirectoryExists("nvmefs:///tmp"));
}

TEST_F(DiskInteractionTest, RemoveDirectoryGivenTemporyDirectoyRemovesSuccessfully) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	// Write a file to temporary folder
	string temp_filename = "nvmefs:///tmp/file";
	vector<char> buf {'H', 'E', 'L', 'L', 'O'};
	fh = file_system->OpenFile(temp_filename, flags);
	fh->Write(buf.data(), buf.size());

	// Verify that it exists
	EXPECT_TRUE(file_system->FileExists(temp_filename));

	// Remove tmp directory and confirm deletion
	EXPECT_NO_THROW(file_system->RemoveDirectory("nvmefs:///tmp"));
	EXPECT_FALSE(file_system->FileExists(temp_filename));
}

TEST_F(DiskInteractionTest, RemoveDirectoryGivenInvalidDirectoryThrowsIOException) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	EXPECT_THROW(file_system->RemoveDirectory("nvmefs://test.db/mydirectory"), IOException);
}

TEST_F(DiskInteractionTest, CreateDirectoryThrowsExpectionIfMetadataNotLoaded) {
	EXPECT_THROW(file_system->CreateDirectory("nvmefs:///tmp"), IOException);
}

TEST_F(DiskInteractionTest, CreateDirectoryNoExceptionThrownWhenMetadataLoaded) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	EXPECT_NO_THROW(file_system->CreateDirectory("nvmefs:///tmp"));
}

TEST_F(DiskInteractionTest, RemoveFileGivenWALRemovesWALData) {
	/*
	 * We are going to write two blocks to the WAL, so:
	 * | block1 = Hello | block2 = World | block3 = ? | ... blocks
	 * Next write will target block3, however we delete WAL. Hence
	 * next write will instead target block1
	 */
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	// Write to the blocks
	string wal_filename = "nvmefs://test.db.wal";
	vector<char> buf_hello {'H', 'E', 'L', 'L', 'O'};
	vector<char> buf_world {'W', 'O', 'R', 'L', 'D'};
	fh = file_system->OpenFile(wal_filename, flags);
	fh->Write(buf_hello.data(), buf_hello.size(), 0);
	fh->Write(buf_world.data(), buf_world.size(), 4096); // 4096 = 1 block

	// Read back and confirm
	vector<char> res_hello(buf_hello.size());
	vector<char> res_world(buf_world.size());
	fh->Read(res_hello.data(), buf_hello.size(), 0);
	fh->Read(res_world.data(), buf_world.size(), 4096);
	EXPECT_EQ(res_hello, buf_hello);
	EXPECT_EQ(res_world, buf_world);

	// Remove WAL, write "fresh" and confirm
	EXPECT_NO_THROW(file_system->RemoveFile(wal_filename));
	vector<char> buf_fresh {'F', 'R', 'E', 'S', 'H'};
	vector<char> res_fresh(buf_fresh.size());
	fh->Write(buf_fresh.data(), buf_fresh.size());
	fh->Read(res_fresh.data(), buf_fresh.size(), 0);

	EXPECT_EQ(res_fresh, buf_fresh);
}

TEST_F(DiskInteractionTest, RemoveFileGivenValidTempFileRemovesIt) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	// Write temporary file
	string temp_filename = "nvmefs:///tmp/file";
	vector<char> buf {'H', 'E', 'L', 'L', 'O'};
	fh = file_system->OpenFile(temp_filename, flags);
	fh->Write(buf.data(), buf.size());

	// Verify that it exists
	EXPECT_TRUE(file_system->FileExists(temp_filename));

	// Remove and verify deletion
	file_system->RemoveFile(temp_filename);
	EXPECT_FALSE(file_system->FileExists(temp_filename));
}

TEST_F(DiskInteractionTest, RemoveFileGivenPathBesidesTMPOrWALDoesNothing) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	EXPECT_NO_THROW(file_system->RemoveFile("nvmefs://test.db"));
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

TEST_F(DiskInteractionTest, OpenFileProducesCorrectFileHandle) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);
	NvmeFileHandle &nvme_fh = fh->Cast<NvmeFileHandle>();
	NvmeFileSystem &nvme_fs = nvme_fh.file_system.Cast<NvmeFileSystem>();

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
	string hello = "Hello, World!";
	vector<char> data_ptr {hello.begin(), hello.end()};
	int data_size = data_ptr.size();
	file->Write(data_ptr.data(), data_size, 0);

	// Read the data back
	vector<char> buffer(data_size);
	file->Read(buffer.data(), data_size, 0);

	// Check that the data is correct
	EXPECT_EQ(string(buffer.data(), data_size), hello);
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
	string hello_db = "Hello, db!";
	vector<char> db_data_ptr {hello_db.begin(), hello_db.end()};
	int db_data_size = db_data_ptr.size();
	db_file->Write(db_data_ptr.data(), db_data_size, 0);

	string hello_wal = "Hello, wal!";
	vector<char> wal_data_ptr {hello_wal.begin(), hello_wal.end()};
	int wal_data_size = wal_data_ptr.size();
	wal_file->Write(wal_data_ptr.data(), wal_data_size, 0);

	string hello_tmp = "Hello, tmp!";
	vector<char> tmp_data_ptr {hello_tmp.begin(), hello_tmp.end()};
	int tmp_data_size = tmp_data_ptr.size();
	tmp_file->Write(tmp_data_ptr.data(), tmp_data_size, 0);

	// Read the data back
	vector<char> db_buffer(db_data_size);
	db_file->Read(db_buffer.data(), db_data_size, 0);

	vector<char> wal_buffer(wal_data_size);
	wal_file->Read(wal_buffer.data(), wal_data_size, 0);

	vector<char> tmp_buffer(tmp_data_size);
	tmp_file->Read(tmp_buffer.data(), tmp_data_size, 0);

	// Check that the data is correct
	EXPECT_EQ(string(db_buffer.data(), db_data_size), hello_db);
	EXPECT_EQ(string(wal_buffer.data(), wal_data_size), hello_wal);
	EXPECT_EQ(string(tmp_buffer.data(), tmp_data_size), hello_tmp);
}

// TODO: Make this parameterized to test different byte offsets whithin different blocks
TEST_F(DiskInteractionTest, WriteAndReadDataWithinBlock) {

	// Create a file
	string file_path = "nvmefs://test.db";
	unique_ptr<FileHandle> file =
	    file_system->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	ASSERT_TRUE(file != nullptr);

	// Write some data to the file
	string hello = "Hello, World!";
	vector<char> data_ptr {hello.begin(), hello.end()};
	int data_size = data_ptr.size();
	file->Write(data_ptr.data(), data_size, 16); // Write data at the 16th byte of the device

	// Read the data back
	vector<char> buffer(data_size);
	file->Read(buffer.data(), data_size, 16); // Read data from the 16th byte of the device

	// Check that the data is correct
	EXPECT_EQ(string(buffer.data(), data_size), hello);
}

TEST_F(DiskInteractionTest, WriteAndReadDataWithSeek) {

	// Create a file
	string file_path = "nvmefs://test.db";
	unique_ptr<FileHandle> file =
	    file_system->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	ASSERT_TRUE(file != nullptr);

	idx_t block_location = 4096 * 5; // 5 blocks of 4096 bytes each

	// Write some data to the file
	string hello = "Hello, World!";
	vector<char> data_ptr {hello.begin(), hello.end()};
	int data_size = data_ptr.size();
	file->Write(data_ptr.data(), data_size, block_location);

	// Seek to the beginning of the file
	file->Seek(4096 * 3);

	// The old 5th block should now be translated to the 0th block after seek
	vector<char> buffer(data_size);
	file->Read(buffer.data(), data_size, 4096 * 2);

	// Check that the data is correct
	EXPECT_EQ(string(buffer.data(), data_size), hello);
}

TEST_F(DiskInteractionTest, SeekOutOfDeviceBounds) {
	// Create a file
	string file_path = "nvmefs://test.db";
	unique_ptr<FileHandle> file =
	    file_system->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	ASSERT_TRUE(file != nullptr);

	// Attempt to seek out of bounds
	EXPECT_THROW(file->Seek((1ULL << 31) + 1), std::runtime_error);
}

TEST_F(DiskInteractionTest, SeekOutOfDBMetadataBounds) {
	// Create a file
	string file_path = "nvmefs://test.db";
	unique_ptr<FileHandle> file =
	    file_system->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	ASSERT_TRUE(file != nullptr);

	// Attempt to seek out of bounds
	EXPECT_THROW(file->Seek((1ULL << 31) + 1), std::runtime_error);
}

TEST_F(DiskInteractionTest, SeekOutOfWALMetadataBounds) {

	const uint64_t max_lba = 261503;
	// Ensure that metadata is created
	file_system->OpenFile("nvmefs://test.db", FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);

	// Create a file
	const string file_path = "nvmefs://test.db.wal";
	unique_ptr<FileHandle> file =
	    file_system->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	ASSERT_TRUE(file != nullptr);

	// Attempt to seek out of bounds
	EXPECT_THROW(file->Seek((1 << 25) + 1), std::runtime_error);
}

TEST_F(DiskInteractionTest, SeekOutOfTmpMetadataBounds) {

	file_system->OpenFile("nvmefs://test.db", FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);

	// Create a file
	string file_path =
	    StringUtil::Format("nvmefs://test.db/tmp/duckdb_temp_storage_%d-%llu.tmp", DEFAULT_BLOCK_ALLOC_SIZE, 0);

	unique_ptr<FileHandle> file =
	    file_system->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	ASSERT_TRUE(file != nullptr);

	// Attempt to seek out of bounds
	EXPECT_THROW(file->Seek(4096),
	             std::runtime_error); // It should throw an error because nothing has been written so far in the file?
}

TEST_F(DiskInteractionTest, ReadAndWriteReturningNumberOfBytes) {
	string file_path = "nvmefs://test.db";
	unique_ptr<FileHandle> file =
	    file_system->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	ASSERT_TRUE(file != nullptr);

	// Write some data to the file
	string hello = "Hello, World!";
	vector<char> data_ptr {hello.begin(), hello.end()};
	int data_size = data_ptr.size();
	uint64_t bytes_written = file->Write(data_ptr.data(), 13);

	// Read the data
	vector<char> buffer(13);
	uint64_t bytes_read = file->Read(buffer.data(), 13);

	// Check that the number of bytes written and read is correct
	EXPECT_EQ(bytes_written, 13);
	EXPECT_EQ(bytes_read, 13);
	EXPECT_EQ(string(buffer.data(), bytes_read), hello);
}

TEST_F(DiskInteractionTest, ReadWithReturnOfBytesAfterSettingSeek) {
	string file_path = "nvmefs://test.db";
	unique_ptr<FileHandle> file =
	    file_system->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	ASSERT_TRUE(file != nullptr);

	// Write some data to the file
	string hello = "Hello, World!";
	vector<char> data_ptr {hello.begin(), hello.end()};
	int data_size = data_ptr.size();
	file->Write(data_ptr.data(), data_size, 4096 * 64);

	// Move file pointer to next duckdb page
	file->Seek(4096 * 64);

	// Read the data
	vector<char> buffer(data_size);
	uint64_t bytes_read = file->Read(buffer.data(), data_size);

	// Check that the number of bytes written and read is correct
	EXPECT_EQ(bytes_read, data_size);
	EXPECT_EQ(string(buffer.data(), bytes_read), hello);
}

TEST_F(DiskInteractionTest, WriteOutOfMetadataAssignedLBARangeForDBFile) {
	// Create a file
	const string file_path = "nvmefs://test.db";
	const uint64_t max_lba = 253311;
	unique_ptr<FileHandle> file =
	    file_system->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	ASSERT_TRUE(file != nullptr);

	// Attempt to write data out of lba range
	string hello = "Hello, World!";
	vector<char> data_ptr {hello.begin(), hello.end()};
	int data_size = data_ptr.size();
	EXPECT_THROW(file->Write(data_ptr.data(), data_size, max_lba * 4096), std::runtime_error);
}

TEST_F(DiskInteractionTest, WriteOutOfMetadataAssignedLBARangeForWALFile) {
	// Create a file
	const string file_path = "nvmefs://test.db.wal";
	const uint64_t max_lba = 261503;

	// Ensure that metadata is created
	file_system->OpenFile("nvmefs://test.db", FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);

	unique_ptr<FileHandle> file =
	    file_system->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	ASSERT_TRUE(file != nullptr);

	// Attempt to write data out of lba range
	char *data_ptr = new char[(1ULL << 25) + 4096];
	int data_size = (1ULL << 25) + 4096;
	EXPECT_THROW(file->Write(data_ptr, data_size, max_lba * 4096), std::runtime_error);

	// Clean up
	delete[] data_ptr;
}

TEST_F(DiskInteractionTest, WriteOutOfMetadataAssignedLBARangeForWALFileWithLocationInsideRange) {
	// Create a file
	const string file_path = "nvmefs://test.db.wal";
	const uint64_t max_lba = 261503;

	// Ensure that metadata is created
	file_system->OpenFile("nvmefs://test.db", FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);

	unique_ptr<FileHandle> file =
	    file_system->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	ASSERT_TRUE(file != nullptr);

	// Attempt to write data out of lba range
	int data_size = 4096 * 64;
	char *data_ptr = new char[4096 * 64];
	for (int i = 0; i < 128; i++) {
		file->Write(data_ptr, data_size, i * data_size);
	}

	EXPECT_THROW(file->Write(data_ptr, data_size, data_size * 128), std::runtime_error);
}

TEST_F(DiskInteractionTest, WriteOutOfMetadataAssignedLBARangeForTmpFile) {
	// Create a file
	string file_path =
	    StringUtil::Format("nvmefs://test.db/tmp/duckdb_temp_storage_%d-%llu.tmp", DEFAULT_BLOCK_ALLOC_SIZE, 0);

	// Ensure that metadata is created
	file_system->OpenFile("nvmefs://test.db", FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);

	unique_ptr<FileHandle> file =
	    file_system->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	ASSERT_TRUE(file != nullptr);

	// Attempt to write data out of range
	char *data_ptr = new char[4096 * 64];
	int data_size = 4096 * 641;
	EXPECT_THROW(file->Write(data_ptr, data_size, 0), std::runtime_error);

	delete[] data_ptr;
}

TEST_F(DiskInteractionTest, WriteAndReadInsideTmpFile) {
	// Create a file
	string file_path =
	    StringUtil::Format("nvmefs://test.db/tmp/duckdb_temp_storage_%d-%llu.tmp", DEFAULT_BLOCK_ALLOC_SIZE, 0);

	// Ensure that metadata is created
	file_system->OpenFile("nvmefs://test.db", FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);

	unique_ptr<FileHandle> file =
	    file_system->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	ASSERT_TRUE(file != nullptr);

	// Attempt to write data out of range
	int data_size = 4096 * 64; // One page
	char *data_ptr = new char[data_size];

	for (int i = 0; i < 5; i++) {
		file->Write(data_ptr, data_size, i * data_size);
	}

	string hello = "Hello, World!";
	vector<char> write_buffer {hello.begin(), hello.end()};
	file->Write(write_buffer.data(), write_buffer.size(), data_size * 3);

	vector<char> read_buffer(write_buffer.size());
	file->Read(read_buffer.data(), read_buffer.size(), data_size * 3);

	// Check that the data is correct
	EXPECT_EQ(string(read_buffer.data(), read_buffer.size()), hello);

	delete[] data_ptr;
}

class BlockManagerTest : public testing::Test {
protected:
	BlockManagerTest() {
		// Set up the test environment
		block_manager = make_uniq<NvmeTemporaryBlockManager>(0, 1024);
	}

	unique_ptr<NvmeTemporaryBlockManager> block_manager;
};

TEST_F(BlockManagerTest, FirstAllocateBlock) {

	// Allocate a block of size 8
	TemporaryBlock *block = block_manager->AllocateBlock(8);

	// Check that the block is allocated correctly
	EXPECT_EQ(block->GetStartLBA(), 0);
	EXPECT_EQ(block->GetEndLBA(), 7);
	EXPECT_EQ(block->GetSizeInBytes(), 8 * 4096);
	EXPECT_EQ(block->IsFree(), false);
}

TEST_F(BlockManagerTest, AllocateTwiceInARow) {

	// Allocate a block of size 8
	TemporaryBlock *block = block_manager->AllocateBlock(8);
	TemporaryBlock *block2 = block_manager->AllocateBlock(8);

	// Check that the block is allocated correctly
	EXPECT_EQ(block->GetStartLBA(), 0);
	EXPECT_EQ(block->GetEndLBA(), 7);
	EXPECT_EQ(block->GetSizeInBytes(), 8 * 4096);
	EXPECT_EQ(block->IsFree(), false);

	EXPECT_EQ(block2->GetStartLBA(), 8);
	EXPECT_EQ(block2->GetEndLBA(), 15);
	EXPECT_EQ(block2->GetSizeInBytes(), 8 * 4096);
	EXPECT_EQ(block2->IsFree(), false);
	EXPECT_EQ(block->GetEndLBA() + 1, block2->GetStartLBA());
}

TEST_F(BlockManagerTest, AllocateFreeAndAllocateAgainYieldsSameBlock) {

	// Allocate a block of size 8
	TemporaryBlock *block = block_manager->AllocateBlock(8);

	// Check that the block is allocated correctly
	idx_t start_lba = block->GetStartLBA();
	idx_t end_lba = block->GetEndLBA();
	idx_t size = block->GetSizeInBytes();
	bool is_free = block->IsFree();

	block_manager->FreeBlock(block);
	TemporaryBlock *block2 = block_manager->AllocateBlock(8);

	EXPECT_EQ(block2->GetStartLBA(), start_lba);
	EXPECT_EQ(block2->GetEndLBA(), end_lba);
	EXPECT_EQ(block2->GetSizeInBytes(), size);
	EXPECT_EQ(block2->IsFree(), is_free);
}

TEST_F(BlockManagerTest,
       AllocateSameSizeThreeTimesFreeTheMiddleAllocationAndAllocateALargerObjectYieldsBlockAfterBlock3) {

	// Allocate a block of size 8
	TemporaryBlock *block = block_manager->AllocateBlock(8);
	TemporaryBlock *block2 = block_manager->AllocateBlock(8);
	TemporaryBlock *block3 = block_manager->AllocateBlock(8);

	ASSERT_TRUE(block->GetStartLBA() == 0);
	ASSERT_TRUE(block2->GetStartLBA() == block->GetEndLBA() + 1);
	ASSERT_TRUE(block3->GetStartLBA() == block2->GetEndLBA() + 1);
	ASSERT_TRUE(block3->GetEndLBA() == block3->GetStartLBA() + 7);

	// Check that the block is allocated correctly
	idx_t start_lba = block2->GetStartLBA();
	idx_t end_lba = block2->GetEndLBA();

	block_manager->FreeBlock(block2);
	TemporaryBlock *block4 = block_manager->AllocateBlock(16);

	EXPECT_EQ(block4->GetStartLBA(), block3->GetEndLBA() + 1);
	EXPECT_EQ(block4->GetEndLBA(), block4->GetStartLBA() + 15);
	EXPECT_EQ(block4->GetSizeInBytes(), 16 * 4096);
	EXPECT_EQ(block4->IsFree(), false);
}

TEST_F(BlockManagerTest,
       AllocateFreeBlocksAndFreeSurroundingBlocksAndAllocateALargerBlockYieldsBlockThatStartsFromSameLocation) {

	// Allocate a block of size 8
	TemporaryBlock *block = block_manager->AllocateBlock(8);
	TemporaryBlock *block2 = block_manager->AllocateBlock(8);
	TemporaryBlock *block3 = block_manager->AllocateBlock(8);

	ASSERT_TRUE(block->GetStartLBA() == 0);
	ASSERT_TRUE(block2->GetStartLBA() == block->GetEndLBA() + 1);
	ASSERT_TRUE(block3->GetStartLBA() == block2->GetEndLBA() + 1);
	ASSERT_TRUE(block3->GetEndLBA() == block3->GetStartLBA() + 7);

	// Check that the block is allocated correctly
	block_manager->FreeBlock(block);  // Should not coalesce anything
	block_manager->FreeBlock(block3); // Should coalesce with the original large block
	block_manager->FreeBlock(block2); // Should coalesce with block and block3 and move it to the large block free list

	printf("All blocks freed\n");

	TemporaryBlock *block4 = block_manager->AllocateBlock(24);

	EXPECT_EQ(block4->GetStartLBA(), 0);
	EXPECT_EQ(block4->GetEndLBA(), 23);
	EXPECT_EQ(block4->GetSizeInBytes(), 24 * 4096);
	EXPECT_EQ(block4->IsFree(), false);
}
// TODO: Create a test that allocates three blocks and deallocate them one by one. In the end allocate all of them again
// in one block and see that it has been consolidated correctly

} // namespace duckdb
