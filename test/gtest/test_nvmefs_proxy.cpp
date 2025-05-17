#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "nvmefs.hpp"
#include "nvmefs_config.hpp"
#include "nvmefs_temporary_block_manager.hpp"
#include "utils/gtest_utils.hpp"
#include "utils/fake_device.hpp"

using ::testing::UnorderedElementsAre;

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
		    .max_temp_size = 32000 * block_size + 64000 * block_size,
		    .max_wal_size = 1ULL << 25 // 32 MiB
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
	FileOpenFlags flags =
	    FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	// Write something to create file
	string hello = "hello temp";
	vector<char> hello_buf {hello.begin(), hello.end()};
	int bytes_to_read_write = hello.size();
	string tmp_file_path = StringUtil::Format("nvmefs:///tmp/duckdb_temp_storage_%s-%llu.tmp", "S32K", 0);
	fh = file_system->OpenFile(tmp_file_path, flags);
	fh->Write(hello_buf.data(), bytes_to_read_write);

	// Check if it exists
	bool exists = file_system->FileExists(tmp_file_path);
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
	FileOpenFlags flags =
	    FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);
	DeviceGeometry geo = file_system->GetDevice().GetDeviceGeometry();

	// Open two temporary files, write to the second
	string tmp_file_path1 = StringUtil::Format("nvmefs:///tmp/duckdb_temp_storage_%s-%llu.tmp", "S32K", 0);
	string tmp_file_path2 = StringUtil::Format("nvmefs:///tmp/duckdb_temp_storage_%s-%llu.tmp", "S64k", 0);
	unique_ptr<FileHandle> tmp_fh_1 = file_system->OpenFile(tmp_file_path1, flags);
	unique_ptr<FileHandle> tmp_fh_2 = file_system->OpenFile(tmp_file_path2, flags);

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
	FileOpenFlags flags =
	    FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	// Write a file to temporary folder
	string temp_filename = StringUtil::Format("nvmefs:///tmp/duckdb_temp_storage_%s-%llu.tmp", "S32K", 0);
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
	FileOpenFlags flags =
	    FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	// Write temporary file

	string temp_filename = StringUtil::Format("nvmefs:///tmp/duckdb_temp_storage_%s-%llu.tmp", "S32K", 0);
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
	string tmp_file_path = StringUtil::Format("nvmefs:///tmp/duckdb_temp_storage_%s-%llu.tmp", "DEFAULT", 0);
	unique_ptr<FileHandle> db_file = file_system->OpenFile(
	    file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_FILE_CREATE);
	ASSERT_TRUE(db_file != nullptr);

	unique_ptr<FileHandle> wal_file = file_system->OpenFile(
	    wal_file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_FILE_CREATE);
	ASSERT_TRUE(wal_file != nullptr);

	unique_ptr<FileHandle> tmp_file = file_system->OpenFile(
	    tmp_file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_FILE_CREATE);
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
	string file_path = StringUtil::Format("nvmefs://test.db/tmp/duckdb_temp_storage_%s-%llu.tmp", "DEFAULT", 0);

	unique_ptr<FileHandle> file = file_system->OpenFile(
	    file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_FILE_CREATE);
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
	string file_path = StringUtil::Format("nvmefs://test.db/tmp/duckdb_temp_storage_%s-%llu.tmp", "DEFAULT", 0);

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

TEST_F(DiskInteractionTest, TrimUnwrittenLocationInFile) {
	int page_size = 4096 * 64; // One page

	// Create a file
	string file_path = "nvmefs://test.db";
	unique_ptr<FileHandle> file =
	    file_system->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);

	ASSERT_TRUE(file->GetFileSize() == 0);

	// Attempt to write data out of range
	file->Trim(0, page_size * 2);

	EXPECT_EQ(file->GetFileSize(), page_size * 2);
}

TEST_F(DiskInteractionTest, TrimWrittenLocationInFileRemovesWrittenDataButKeepsSize) {
	int page_size = 4096 * 64; // One page

	// Create a file
	string file_path = "nvmefs://test.db";
	unique_ptr<FileHandle> file =
	    file_system->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);

	ASSERT_TRUE(file != nullptr);

	// Write some data to the file
	string hello = "Hello, World!";
	vector<char> data_ptr {hello.begin(), hello.end()};
	int data_size = data_ptr.size();
	file->Write(data_ptr.data(), data_size, page_size * 4);

	// Read the data back
	vector<char> buffer(data_size);
	file->Read(buffer.data(), data_size, page_size * 4);
	// Check that the data is correct
	EXPECT_EQ(string(buffer.data(), data_size), hello);

	file->Trim(page_size * 4, data_size);

	memset(buffer.data(), 0, data_size);
	file->Read(buffer.data(), data_size, page_size * 4);

	// Check that the data is correct
	EXPECT_NE(string(buffer.data(), data_size), hello);
	EXPECT_EQ(file->GetFileSize(), page_size * 4 + 4096); // 4 pages + 1 lba
}

TEST_F(DiskInteractionTest, TrimWrittenLocationInFileFromSeekPositionRemovesWrittenDataButKeepsSize) {
	int page_size = 4096 * 64; // One page

	// Create a file
	string file_path = "nvmefs://test.db";
	unique_ptr<FileHandle> file =
	    file_system->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);

	ASSERT_TRUE(file != nullptr);

	// Write some data to the file
	string hello = "Hello, World!";
	vector<char> data_ptr {hello.begin(), hello.end()};
	int data_size = data_ptr.size();
	file->Write(data_ptr.data(), data_size, page_size * 4); // Write data at the 16th byte of the device

	// Read the data back
	vector<char> buffer(data_size);
	file->Read(buffer.data(), data_size, page_size * 4); // Read data from the 16th byte of the device

	// Check that the data is correct
	EXPECT_EQ(string(buffer.data(), data_size), hello);

	file->Seek(page_size * 3);

	file->Trim(page_size, data_size);

	memset(buffer.data(), 0, data_size);
	file->Read(buffer.data(), data_size, page_size * 4); // Read data from the 16th byte of the device

	// Check that the data is correct
	EXPECT_NE(string(buffer.data(), data_size), hello);
	EXPECT_EQ(file->GetFileSize(), page_size * 4 + 4096); // 4 pages + 1 lba
}

TEST_F(DiskInteractionTest, WriteAndReadInsideTmpFile) {
	// Create a file
	string file_path = StringUtil::Format("nvmefs://test.db/tmp/duckdb_temp_storage_%s-%llu.tmp", "S32K", 0);

	// Ensure that metadata is created
	file_system->OpenFile("nvmefs://test.db", FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);

	unique_ptr<FileHandle> file = file_system->OpenFile(
	    file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_FILE_CREATE);
	ASSERT_TRUE(file != nullptr);

	// Attempt to write data out of range
	int data_size = 32768; // One page
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

TEST_F(DiskInteractionTest, NewlyCreatedHandleOffsetRemainZeroAfterReset) {
	unique_ptr<FileHandle> fh =
	    file_system->OpenFile("nvmefs://test.db", FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	NvmeFileHandle &nvme_fh = fh->Cast<NvmeFileHandle>();

	EXPECT_EQ(file_system->SeekPosition(nvme_fh), 0);

	file_system->Reset(nvme_fh);

	// Offset still 0
	EXPECT_EQ(file_system->SeekPosition(nvme_fh), 0);
}

TEST_F(DiskInteractionTest, HandleWithOffsetEqualsZeroWhenReset) {
	unique_ptr<FileHandle> fh =
	    file_system->OpenFile("nvmefs://test.db", FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	NvmeFileHandle &nvme_fh = fh->Cast<NvmeFileHandle>();

	// Seek 5k bytes into file
	file_system->Seek(nvme_fh, 5000);
	EXPECT_EQ(file_system->SeekPosition(nvme_fh), 5000);

	file_system->Reset(nvme_fh);

	// Ensure that offset is zero
	EXPECT_EQ(file_system->SeekPosition(nvme_fh), 0);
}

TEST_F(DiskInteractionTest, ListFilesOfPrefixDirectoryYieldsCorrectFilesAndDirStatus) {
	const string db_filename = "test.db";
	const string wal_filename = "test.db.wal";
	const string tmp_dir_filepath = "/tmp";

	unique_ptr<FileHandle> fh =
	    file_system->OpenFile("nvmefs://test.db", FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);

	vector<std::tuple<string, bool>> results;

	std::function<void(const string &, bool)> lister = [&results](const string &directory, bool is_dir) {
		results.push_back(std::make_tuple(directory, is_dir));
	};

	bool dir = file_system->ListFiles("nvmefs://", lister);

	EXPECT_EQ(dir, true);
	EXPECT_THAT(results,
	            UnorderedElementsAre(std::make_tuple(db_filename, false), std::make_tuple(tmp_dir_filepath, true),
	                                 std::make_tuple(wal_filename, false)));
}

TEST_F(DiskInteractionTest, ListFilesOfTemporaryDirectoryWithFilesYieldCorrectListOfFiles) {
	const string tmp_dir_filepath = "nvmefs:///tmp";

	FileOpenFlags flags =
	    FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	// Open two temporary files, write to both and verify
	string tmp_file_path1 = StringUtil::Format("nvmefs:///tmp/duckdb_temp_storage_%s-%llu.tmp", "S32K", 0);
	string tmp_file_path2 = StringUtil::Format("nvmefs:///tmp/duckdb_temp_storage_%s-%llu.tmp", "S64K", 0);
	unique_ptr<FileHandle> tmp_fh_1 = file_system->OpenFile(tmp_file_path1, flags);
	unique_ptr<FileHandle> tmp_fh_2 = file_system->OpenFile(tmp_file_path2, flags);

	vector<char> buf_h {'H', 'E', 'L', 'L', 'O'};
	tmp_fh_1->Write(buf_h.data(), buf_h.size(), 0);
	tmp_fh_2->Write(buf_h.data(), buf_h.size(), 0);

	vector<char> res1_h(buf_h.size());
	vector<char> res2_h(buf_h.size());
	tmp_fh_1->Read(res1_h.data(), buf_h.size(), 0);
	tmp_fh_2->Read(res2_h.data(), buf_h.size(), 0);

	EXPECT_EQ(res1_h, buf_h);
	EXPECT_EQ(res2_h, buf_h);

	vector<std::tuple<string, bool>> results;

	std::function<void(const string &, bool)> lister = [&results](const string &directory, bool is_dir) {
		results.push_back(std::make_tuple(directory, is_dir));
	};

	bool dir = file_system->ListFiles(tmp_dir_filepath, lister);

	EXPECT_EQ(dir, true);
	EXPECT_THAT(results, UnorderedElementsAre(std::make_tuple("duckdb_temp_storage_S32K-0.tmp", false),
	                                          std::make_tuple("duckdb_temp_storage_S64K-0.tmp", false)));
}

TEST_F(DiskInteractionTest, ListFilesOfEmptyTemporaryDirectoryReturnsNothing) {
	const string tmp_dir_filepath = "nvmefs:///tmp";

	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	vector<std::tuple<string, bool>> results;

	std::function<void(const string &, bool)> lister = [&results](const string &directory, bool is_dir) {
		results.push_back(std::make_tuple(directory, is_dir));
	};

	bool dir = file_system->ListFiles(tmp_dir_filepath, lister);

	EXPECT_EQ(dir, true);
	EXPECT_EQ(results.empty(), true);
}

TEST_F(DiskInteractionTest, ListFilesOfNonDirectoryPathReturnsFalse) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	vector<std::tuple<string, bool>> results;

	std::function<void(const string &, bool)> lister = [&results](const string &directory, bool is_dir) {
		results.push_back(std::make_tuple(directory, is_dir));
	};

	bool dir = file_system->ListFiles("nvmefs://mumblejumbles", lister);

	EXPECT_EQ(dir, false);
	EXPECT_EQ(results.empty(), true);
}

TEST_F(DiskInteractionTest, GetAvailableDiskSpaceDefaultDirWithNoAllocationReturnsCorrectSize) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	DeviceGeometry geo = file_system->GetDevice().GetDeviceGeometry();

	// We allocate 1 LBA for global metadata and
	// SingleFileBlockManager::CreateNewDatabase() writes 3 headers (3 LBAs)

	idx_t expected_size = geo.lba_count * geo.lba_size - (4 * geo.lba_size);
	optional_idx result = file_system->GetAvailableDiskSpace("nvmefs://");
	ASSERT_TRUE(result.IsValid());
	EXPECT_EQ(result.GetIndex(), expected_size);
}

TEST_F(DiskInteractionTest, GetAvailableDiskSpaceDefaultDirWritesInTmpAndWalReturnsCorrectSize) {
	FileOpenFlags flags =
	    FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	DeviceGeometry geo = file_system->GetDevice().GetDeviceGeometry();

	// We allocate 1 LBA for global metadata and
	// SingleFileBlockManager::CreateNewDatabase() writes 3 headers (3 LBAs)

	// Temp file with 2 LBAs and WAL with 1 LBA written
	idx_t expected_size = (geo.lba_count * geo.lba_size) - (2 * geo.lba_size) - geo.lba_size - (4 * geo.lba_size);

	// Allocate files and write to them
	string tmp_file_path1 = StringUtil::Format("nvmefs:///tmp/duckdb_temp_storage_%s-%llu.tmp", "S32K", 0);
	unique_ptr<FileHandle> tmp_fh = file_system->OpenFile(tmp_file_path1, flags);
	unique_ptr<FileHandle> wal_fh = file_system->OpenFile("nvmefs://test.db.wal", flags);
	vector<char> tmp_buf(2 * geo.lba_size);
	vector<char> wal_buf(geo.lba_size);
	memset(tmp_buf.data(), 1, 2 * geo.lba_size);
	memset(wal_buf.data(), 1, geo.lba_size);
	tmp_fh->Write(tmp_buf.data(), 2 * geo.lba_size);
	wal_fh->Write(wal_buf.data(), geo.lba_size);

	// Get size and evaluate
	optional_idx result_size = file_system->GetAvailableDiskSpace("nvmefs://");
	ASSERT_TRUE(result_size.IsValid());
	EXPECT_EQ(result_size.GetIndex(), expected_size);
}

TEST_F(DiskInteractionTest, GetAvailableDiskSpaceTmpDirectoryWithTwoFilesReturnCorrectSize) {
	FileOpenFlags flags =
	    FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	DeviceGeometry geo = file_system->GetDevice().GetDeviceGeometry();

	// Two temp files with 2 and 3 LBAs written to them
	// Allocate files and write to them
	string tmp_file_path1 = StringUtil::Format("nvmefs:///tmp/duckdb_temp_storage_%s-%llu.tmp", "S32K", 0);
	string tmp_file_path2 = StringUtil::Format("nvmefs:///tmp/duckdb_temp_storage_%s-%llu.tmp", "S64K", 0);
	unique_ptr<FileHandle> test1_fh = file_system->OpenFile(tmp_file_path1, flags);
	unique_ptr<FileHandle> test2_fh = file_system->OpenFile(tmp_file_path2, flags);
	vector<char> test1_buf(3 * geo.lba_size);
	vector<char> test2_buf(2 * geo.lba_size);
	memset(test1_buf.data(), 1, 3 * geo.lba_size);
	memset(test2_buf.data(), 1, 2 * geo.lba_size);
	test1_fh->Write(test1_buf.data(), 3 * geo.lba_size);
	test2_fh->Write(test2_buf.data(), 2 * geo.lba_size);

	// check
	// Cheating a bit - max_temp_size is private.
	// The temp dir size for the tests is 640 LBAs
	idx_t expected_size = (32000 * 4096 + 64000 * 4096) - (3 * geo.lba_size) - (2 * geo.lba_size);
	optional_idx result_size = file_system->GetAvailableDiskSpace("nvmefs:///tmp");
	ASSERT_TRUE(result_size.IsValid());
	EXPECT_EQ(result_size.GetIndex(), expected_size);
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

	TemporaryBlock *block4 = block_manager->AllocateBlock(24);

	EXPECT_EQ(block4->GetStartLBA(), 0);
	EXPECT_EQ(block4->GetEndLBA(), 23);
	EXPECT_EQ(block4->GetSizeInBytes(), 24 * 4096);
	EXPECT_EQ(block4->IsFree(), false);
}

TEST_F(
    BlockManagerTest,
    AllocateBlocksAndFreeTheMiddleBlockToTriggerCoalescingToTheLeftAndAcquireANewBlockThatShouldStartFromTheSameLocation) {

	// Allocate a block of size 8
	TemporaryBlock *block = block_manager->AllocateBlock(8);
	TemporaryBlock *block2 = block_manager->AllocateBlock(8);
	TemporaryBlock *block3 = block_manager->AllocateBlock(8);

	ASSERT_TRUE(block->GetStartLBA() == 0);
	ASSERT_TRUE(block2->GetStartLBA() == block->GetEndLBA() + 1);
	ASSERT_TRUE(block3->GetStartLBA() == block2->GetEndLBA() + 1);
	ASSERT_TRUE(block3->GetEndLBA() == block3->GetStartLBA() + 7);

	// Check that the block is allocated correctly
	block_manager->FreeBlock(block); // Should not coalesce anything
	block_manager->FreeBlock(block2);
	block_manager->FreeBlock(block3); // Should coalesce with the original large block

	TemporaryBlock *block4 = block_manager->AllocateBlock(16);

	EXPECT_EQ(block4->GetStartLBA(), 0);
	EXPECT_EQ(block4->GetEndLBA(), 15);
	EXPECT_EQ(block4->GetSizeInBytes(), 16 * 4096);
	EXPECT_EQ(block4->IsFree(), false);
}

TEST_F(BlockManagerTest, FreelistRemoveOneAtATime) {

	// Allocate a block of size 8
	TemporaryBlock *block1 = block_manager->AllocateBlock(8);
	TemporaryBlock *block2 = block_manager->AllocateBlock(8);
	TemporaryBlock *block3 = block_manager->AllocateBlock(8);
	TemporaryBlock *block4 = block_manager->AllocateBlock(8);
	TemporaryBlock *block50 = block_manager->AllocateBlock(8);

	ASSERT_TRUE(block4->GetStartLBA() == block3->GetEndLBA() + 1);

	// Check that the block is allocated correctly
	block_manager->FreeBlock(block2);
	block_manager->FreeBlock(block4); // Should coalesce with the original large block

	TemporaryBlock *block = block_manager->AllocateBlock(8);

	EXPECT_EQ(block->GetStartLBA(), 24);
	EXPECT_EQ(block->GetEndLBA(), 31);
	EXPECT_EQ(block->GetSizeInBytes(), 8 * 4096);
	EXPECT_EQ(block->IsFree(), false);

	TemporaryBlock *block5 = block_manager->AllocateBlock(8);

	EXPECT_EQ(block5->GetStartLBA(), 8);
	EXPECT_EQ(block5->GetEndLBA(), 15);
	EXPECT_EQ(block5->GetSizeInBytes(), 8 * 4096);
	EXPECT_EQ(block5->IsFree(), false);
}

TEST_F(BlockManagerTest, FreelistCoalesceLeftAndRightInTheMiddle) {

	// Allocate a block of size 8
	TemporaryBlock *block1 = block_manager->AllocateBlock(8);
	TemporaryBlock *block2 = block_manager->AllocateBlock(8);
	TemporaryBlock *block3 = block_manager->AllocateBlock(8);
	TemporaryBlock *block4 = block_manager->AllocateBlock(8);
	TemporaryBlock *block5 = block_manager->AllocateBlock(8);
	TemporaryBlock *block6 = block_manager->AllocateBlock(8);
	TemporaryBlock *block7 = block_manager->AllocateBlock(8);
	TemporaryBlock *block8 = block_manager->AllocateBlock(8);
	TemporaryBlock *block9 = block_manager->AllocateBlock(8);
	TemporaryBlock *block10 = block_manager->AllocateBlock(8);
	TemporaryBlock *block50 = block_manager->AllocateBlock(8);

	ASSERT_TRUE(block4->GetStartLBA() == block3->GetEndLBA() + 1);

	// Check that the block is allocated correctly
	block_manager->FreeBlock(block5);
	block_manager->FreeBlock(block7); // Should coalesce with the original large block
	block_manager->FreeBlock(block6);

	TemporaryBlock *block = block_manager->AllocateBlock(16);

	EXPECT_EQ(block->GetStartLBA(), 32);
	EXPECT_EQ(block->GetEndLBA(), 47);
	EXPECT_EQ(block->GetSizeInBytes(), 16 * 4096);
	EXPECT_EQ(block->IsFree(), false);

	TemporaryBlock *block11 = block_manager->AllocateBlock(8);
	EXPECT_EQ(block11->GetStartLBA(), 48);
	EXPECT_EQ(block11->GetEndLBA(), 55);
	EXPECT_EQ(block11->GetSizeInBytes(), 8 * 4096);
	EXPECT_EQ(block11->IsFree(), false);
}

TEST_F(BlockManagerTest, FreelistCoalesceLeftInTheMiddleOfTheList) {

	// Allocate a block of size 8
	TemporaryBlock *block1 = block_manager->AllocateBlock(8);
	TemporaryBlock *block2 = block_manager->AllocateBlock(8);
	TemporaryBlock *block3 = block_manager->AllocateBlock(8);
	TemporaryBlock *block4 = block_manager->AllocateBlock(8);
	TemporaryBlock *block5 = block_manager->AllocateBlock(8);
	TemporaryBlock *block6 = block_manager->AllocateBlock(8);
	TemporaryBlock *block7 = block_manager->AllocateBlock(8);
	TemporaryBlock *block8 = block_manager->AllocateBlock(8);
	TemporaryBlock *block9 = block_manager->AllocateBlock(8);
	TemporaryBlock *block10 = block_manager->AllocateBlock(8);
	TemporaryBlock *block50 = block_manager->AllocateBlock(8);

	ASSERT_TRUE(block4->GetStartLBA() == block3->GetEndLBA() + 1);

	// Check that the block is allocated correctly
	block_manager->FreeBlock(block5);
	block_manager->FreeBlock(block6);
	block_manager->FreeBlock(block7);

	TemporaryBlock *block = block_manager->AllocateBlock(16);

	EXPECT_EQ(block->GetStartLBA(), 32);
	EXPECT_EQ(block->GetEndLBA(), 47);
	EXPECT_EQ(block->GetSizeInBytes(), 16 * 4096);
	EXPECT_EQ(block->IsFree(), false);

	TemporaryBlock *block11 = block_manager->AllocateBlock(8);
	EXPECT_EQ(block11->GetStartLBA(), 48);
	EXPECT_EQ(block11->GetEndLBA(), 55);
	EXPECT_EQ(block11->GetSizeInBytes(), 8 * 4096);
	EXPECT_EQ(block11->IsFree(), false);
}

} // namespace duckdb
