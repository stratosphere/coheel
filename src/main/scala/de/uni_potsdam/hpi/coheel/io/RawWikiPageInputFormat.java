package de.uni_potsdam.hpi.coheel.io;

import de.uni_potsdam.hpi.coheel.wiki.RawWikiPage;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;

/**
 * These classes are heavily inspired by the work of Jimmy Lin:
 * "Cloud9: A MapReduce Library for Hadoop"  http://cloud9lib.org/ licenced under
 * Apache License, Version 2.0 http://www.apache.org/licenses/LICENSE-2.0.
 * The original code can be found at  https://github.com/lintool/Cloud9.
 *
 * The class is meant to parse a Wikipedia XML dump
 *
 * @author Jimmy Lin
 * @author tongr
 *
 * @see <a href="http://cloud9lib.org/">http://cloud9lib.org/</a>
 * @see <a href="https://github.com/lintool/Cloud9">https://github.com/lintool/Cloud9</a>
 */
public class RawWikiPageInputFormat extends FileInputFormat<LongWritable, RawWikiPage> {
	/**
	 * Start delimiter of the page, which is &lt;<code>page</code>&gt;.
	 */
	private static final String XML_START_TAG = "<page>";

	/**
	 * End delimiter of the page, which is &lt;<code>/page</code>&gt;.
	 */
	private static final String XML_END_TAG = "</page>";

	/**
	 * Start delimiter of the title, which is &lt;<code>title</code>&gt;.
	 */
	private static final String XML_START_TAG_TITLE = "<title>";

	/**
	 * End delimiter of the title, which is &lt;<code>/title</code>&gt;.
	 */
	private static final String XML_END_TAG_TITLE = "</title>";

	/**
	 * Start delimiter of the namespace, which is &lt;<code>ns</code>&gt;.
	 */
	private static final String XML_START_TAG_NAMESPACE = "<ns>";

	/**
	 * End delimiter of the namespace, which is &lt;<code>/ns</code>&gt;.
	 */
	private static final String XML_END_TAG_NAMESPACE = "</ns>";

	/**
	 * Start delimiter of the text, which is &lt;<code>text xml:space=\"preserve\"</code>&gt;.
	 */
	private static final String XML_START_TAG_TEXT = "<text xml:space=\"preserve\">";

	/**
	 * End delimiter of the text, which is &lt;<code>/text</code>&gt;.
	 */
	private static final String XML_END_TAG_TEXT = "</text>";

	/**
	 * Start delimiter of the text, which is &lt;<code>edirect title\"</code>.
	 */
	private static final String XML_START_TAG_REDIRECT = "<redirect title=\"";

	/**
	 * End delimiter of the text, which is <code>\" /</code>&gt;.
	 */
	private static final String XML_END_TAG_REDIRECT = "\" />";
	@Override
	public RecordReader<LongWritable, RawWikiPage> createRecordReader(
		InputSplit split, TaskAttemptContext context) throws IOException,
		InterruptedException {
		return /*new RawWikiPageRecordReader();
		/*/
			new WikiRecordReader();
		//*/
	}

	private static class WikiRecordReader extends RecordReader<LongWritable, RawWikiPage> {
		private byte[] startTag;
		private byte[] endTag;
		private long start;
		private long end;
		private long pos;
		private DataInputStream fsin = null;
		private DataOutputBuffer buffer = new DataOutputBuffer();

		private final LongWritable key = new LongWritable();
		private RawWikiPage value;

		@Override
		public void initialize(InputSplit input, TaskAttemptContext context)
			throws IOException, InterruptedException {
			startTag = XML_START_TAG.getBytes("utf-8");
			endTag = XML_END_TAG.getBytes("utf-8");

			// get current file split
			FileSplit split = (FileSplit) input;
			start = split.getStart();
			end = start + split.getLength();
			pos = start;

			// seek start of the split
			Path file = split.getPath();
			FileSystem fs = file.getFileSystem(context.getConfiguration());
			FSDataInputStream fileIn = fs.open(file);
			fileIn.seek(start);
			fsin = fileIn;
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (pos < end) {
				if (readUntilMatch(startTag, false)) {

					try {
						buffer.write(startTag);
						if (readUntilMatch(endTag, true)) {
							key.set(pos - startTag.length);
							parseRawWikiPage();
							return true;
						}
					} finally {
						// original comment of Jimmy Lin:
						// Because input streams of gzipped files are not seekable, we need to keep track of
						// bytes consumed ourselves.
						// This is a sanity check to make sure our internal computation of bytes consumed is
						// accurate. This should be removed later for efficiency once we confirm that this code
						// works correctly.
						assert
							!( fsin instanceof Seekable ) ||
							pos == ((Seekable) fsin).getPos():
									"bytes consumed error!";

						buffer.reset();
					}
				}
			}
			return false;
		}

		@Override
		public LongWritable getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		@Override
		public RawWikiPage getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public void close() throws IOException {
			fsin.close();
		}

		@Override
		public float getProgress() throws IOException {
			return ((float) (pos - start)) / ((float) (end - start));
		}

		private boolean readUntilMatch(byte[] match, boolean withinBlock)
			throws IOException {
			int i = 0;
			while (true) {
				int b = fsin.read();
				// increment position (bytes consumed)
				pos++;

				// end of file:
				if (b == -1)
					return false;
				// save to buffer:
				if (withinBlock)
					buffer.write(b);

				// check if we're matching:
				if (b == match[i]) {
					i++;
					if (i >= match.length)
						return true;
				} else
					i = 0;
				// see if we've passed the stop point:
				if (!withinBlock && i == 0 && pos >= end)
					return false;
			}
		}

		private void parseRawWikiPage() throws CharacterCodingException {
			String pageXml = Text.decode(buffer.getData(), 0, buffer.getLength());

			// parse page content
			final String pageTitle = escapedContent(XML_START_TAG_TITLE, XML_END_TAG_TITLE, pageXml);
			final int ns = intContent0(XML_START_TAG_NAMESPACE, XML_END_TAG_NAMESPACE, pageXml);
			final String redirectTitle = escapedContent(XML_START_TAG_REDIRECT, XML_END_TAG_REDIRECT, pageXml);
			final String source = escapedContent(XML_START_TAG_TEXT, XML_END_TAG_TEXT, pageXml);

			value = new RawWikiPage(
				pageTitle,
				ns,
				redirectTitle,
				source
			);
		}

		private String escapedContent(String startTag, String endTag, String xml) {
			return StringEscapeUtils.unescapeXml( getContent(startTag, endTag, xml, "") );
		}

		private int intContent0(String startTag, String endTag, String xml) {
			return Integer.parseInt( getContent(startTag, endTag, xml, "0") );
		}

		private String getContent(String startTag, String endTag, String xml, String defaultVal) {
			int start = xml.indexOf(startTag);
			if(start<0) {
				return defaultVal;
			}

			int end = xml.indexOf(endTag,
				start);
			return xml.substring(start + startTag.length(), end);
		}
	}
}
