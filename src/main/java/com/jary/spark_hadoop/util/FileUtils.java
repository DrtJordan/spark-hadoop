package com.jary.spark_hadoop.util;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FileUtils {

	@SuppressWarnings("resource")
	private static void writeBytes2File(ByteBuffer bb, File out) throws Exception {
		FileChannel targetChannel = new FileOutputStream(out).getChannel();
		try {
			targetChannel.write(bb);
		} finally {
			targetChannel.close();
		}
	}

	public static boolean move(File srcFile, File destFile) {
		if (srcFile.renameTo(destFile)) {
			return true;
		}
		return false;
	}

	public static boolean copy(File srcFile, File destFile) {
		if (srcFile.isDirectory()) {
			return false;
		}
		File parentPathFile = destFile.getParentFile();
		if (!parentPathFile.exists()) {
			parentPathFile.mkdirs();
		}
		return copyFile(srcFile, destFile);
	}

    /**
     * 删除文件 或  删除目录
     */
    public static boolean delete(String filePath) {
    	File file = new File(filePath);
        if (file.exists()) {
        	return delete(file);
        }
		return false;
        
    }
    public static boolean delete(File file) {
        if (file.isDirectory()) {
            //递归删除目录中的子目录下
            File files[] = file.listFiles();
			for (int i = 0; i < files.length; i++) {
				boolean success = delete(files[i]);
				if (!success) {
                    return false;
                }
			}
        }
        // 目录此时为空，可以删除
        return file.delete();
    }

	private static void createParentDirs(File file) {
		File parentPathFile = file.getParentFile();
		if (!parentPathFile.exists()) {
			parentPathFile.mkdirs();
		}
	}

	public static boolean appendBinaryToFile(String path, byte[] data, int pos) {
		return appendBinaryToFile(new File(path), data, pos);
	}

	public static boolean appendBinaryToFile(File file, byte[] data, int pos) {
		createParentDirs(file);
		try {
			RandomAccessFile rfile = new RandomAccessFile(file, "rw");
			if (pos != 0) {
				rfile.seek(file.length());
			} else {
				rfile.seek(0);
			}
			rfile.write(data);
			rfile.close();
			return true;
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		return false;
	}

	public static File binaryToFile(String path, byte[] data) {
		return binaryToFile(new File(path), data);
	}

	public static File binaryToFile(File file, byte[] data) {
		if (data == null) {
			return null;
		}
		if (file.exists()) {
			if (file.isDirectory()) {
				return null;
			}
		} else {
			File dir = file.getParentFile();
			if (dir != null && !dir.exists()) {
				dir.mkdir();
			}
		}

		ByteBuffer bb = ByteBuffer.wrap(data);
		try {
			writeBytes2File(bb, file);
		} catch (Exception e) {
			return null;
		}
		return file;
	}

	public static File string2file(File file, String data) {
		OutputStream os = null;
		try {
			os = new FileOutputStream(file);
			os.write(data.getBytes());
			return file;
		} catch (Exception ex) {
		} finally {
			if (os != null) {
				try {
					os.close();
				} catch (IOException ex) {
				}
			}
		}
		return null;
	}

	public static String getFileName(String filename) {
		if (filename.endsWith("/")) {
			return "";
		}
		int begin = filename.lastIndexOf("/") + 1;
		int end = filename.length();
		filename = filename.substring(begin, end);
		String fileName;
		int index = filename.lastIndexOf(".");
		if (index >= 0) {
			fileName = filename.substring(0, index);
		} else {
			fileName = filename;
		}
		return fileName;
	}

	static public byte[] binaryFromFile(File file) {
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(file);
			byte[] fileData = new byte[(int) file.length()];
			// convert file into array of bytes
			fis.read(fileData);
			return fileData;
		} catch (Exception e) {
			// e.printStackTrace();
		} finally {
			if (fis != null) {
				try {
					fis.close();
				} catch (IOException ex) {
				}
			}

		}
		return null;
	}

	static public byte[] binaryFromFile(String path) {
		File f = new File(path);
		if (!f.exists()) {
			System.err.println("File (" + path + ") does not exist, read faild!");
			return null;
		}
		return binaryFromFile(f);
	}

	static public boolean mkdir(String dir) {
		File d = new File(dir);
		if (!d.exists()) {
			return d.mkdirs();
		}
		return true;
	}

	public static String parentDirectoryOfFile(File file) {
		String dir = file.getParent();
		if (dir == null) {
			return "";
		}
		return dir + "/";
	}

	public static String nameOfDir(String dirPath) {
		if (dirPath.endsWith(File.separator)) {
			File f = new File(dirPath);
			return f.getName();
		} else {
			File f = new File(dirPath);
			if (f.exists() && f.isDirectory()) {
				return f.getName();
			} else {
				return null;
			}
		}
	}

	public static String nameOfFile(String filePath) {
		if (filePath.endsWith(File.separator)) {
			return null;
		}
		File f = new File(filePath);
		return f.getName();
	}

	static public File[] subDirectories(File dir) {
		if (!dir.exists() || !dir.isDirectory()) {
			return new File[0];
		}
		return dir.listFiles(new FileFilter() {

			@Override
			public boolean accept(File pathname) {
				return pathname.isDirectory();
			}
		});
	}

	static public File[] subFiles(File dir) {
		if (!dir.exists() || !dir.isDirectory()) {
			return new File[0];
		}
		return dir.listFiles(new FileFilter() {

			@Override
			public boolean accept(File pathname) {
				return !pathname.isDirectory() && !pathname.getName().startsWith(".");
			}
		});
	}

	static public List<File> allSubFiles(File dir) {
		List<File> list = new ArrayList<File>();
		for (File d : subDirectories(dir)) {
			list.addAll(allSubFiles(d));
		}
		list.addAll(Arrays.asList(subFiles(dir)));
		return list;
	}

	public static List<String> listFiles(String dir) {
		List<String> lstFileNames = new ArrayList<String>();
		listFiles(lstFileNames, new File(dir));
		return lstFileNames;
	}

	private static void listFiles(List<String> listFileNames, File file) {
		if (file.isDirectory()) {
			File[] t = file.listFiles();

			for (int i = 0; i < t.length; i++) {
				File f = t[i];
				if (f.isDirectory()) {
					listFileNames.add("[" + f.getName() + "]");
				} else {
					listFileNames.add(f.getName());
				}
			}
		}
	}

	public static byte[] readBytes(String path, long pos, int size) {
		RandomAccessFile file = null;
		try {
			file = new RandomAccessFile(path, "r");
			long fileSize = file.length();
			if (pos >= fileSize) {
				return null;
			}

			if (pos + size > fileSize) {
				size = (int) (fileSize - pos);
			}
			file.seek(pos);
			byte[] buffer = new byte[size];
			file.read(buffer);
			return buffer;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (file != null) {
				try {
					file.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return null;
	}

	public static boolean appendBytes(String path, byte data[]) {
		RandomAccessFile file = null;
		try {
			file = new RandomAccessFile(path, "rw");
			file.seek(file.length());
			file.write(data);
			file.close();
			return true;
		} catch (IOException e) {
		} finally {
			if (file != null) {
				try {
					file.close();
				} catch (IOException e) {
				}
			}
		}
		return false;
	}

	public InputStream readFile(String relatedPath) {
		return this.getClass().getResourceAsStream(relatedPath);
	}

	protected String getProperty(String name) {
		Properties props = new Properties();
		String file = "/META-INF/quartz.properties";
		URL fileURL = this.getClass().getResource(file);
		if (fileURL != null) {
			try {
				props.load(this.getClass().getResourceAsStream(file));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return props.getProperty(name);
	}

	public static boolean copyFile(File sourceFile, File destFile) {
		try {
			DataInputStream dis = new DataInputStream(new FileInputStream(sourceFile));
			DataOutputStream dos = new DataOutputStream(new FileOutputStream(destFile));
			byte[] data = new byte[10240];
			int readCount = dis.read(data);
			while (readCount > 0) {
				dos.write(data, 0, readCount);
				readCount = dis.read(data);
			}
			dos.close();
			dis.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	public static String getContent(String fileName, String encoding) {
		try {
			BufferedInputStream bis = new BufferedInputStream(new FileInputStream(fileName));
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			byte[] data = new byte[10240];
			int count = bis.read(data);
			while (count > 0) {
				baos.write(data, 0, count);
				count = bis.read(data);

			}
			bis.close();
			return new String(baos.toByteArray(), encoding);

		} catch (Exception e) {

		}
		return "";
	}

	public static String getContent(File file, String encoding) {
		try {
			BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			byte[] data = new byte[10240];
			int count = bis.read(data);
			while (count > 0) {
				baos.write(data, 0, count);
				count = bis.read(data);

			}
			bis.close();
			return new String(baos.toByteArray(), encoding);

		} catch (Exception e) {

		}
		return "";
	}

	public static String getSubContent(String content, String startToken, String endToken) {
		int start = content.indexOf(startToken);
		if (start >= 0) {
			start += startToken.length();
			int end = content.indexOf(endToken, start);
			if (start >= 0 && end > 0) {
				String value = content.substring(start, end);
				return value;

			} else {
				return "";
			}
		}
		return "";
	}
}
