package org.jumpmind.symmetric.route.parse;

import java.io.PrintStream;
import java.io.PrintWriter;

public class DBFException extends Exception {
	private static final long serialVersionUID = 1L;

	public DBFException(String s) {
		this(s, null);
	}

	public DBFException(Throwable throwable) {
    this(throwable.getMessage(), throwable);
  }

	public DBFException(String s, Throwable throwable) {
		super(s);
		detail = throwable;
	}

	public String getMessage() {
		if (detail == null) {
			return super.getMessage();
		} else {
			return super.getMessage();
		}
	}

	public void printStackTrace(PrintStream printstream) {
		if (detail == null) {
			super.printStackTrace(printstream);
			return;
		}
		PrintStream printstream1 = printstream;
		printstream1.println(this);
		detail.printStackTrace(printstream);
		return;
	}

	public void printStackTrace() {
		printStackTrace(System.err);
	}

	public void printStackTrace(PrintWriter printwriter) {
		if (detail == null) {
			super.printStackTrace(printwriter);
			return;
		}
		PrintWriter printwriter1 = printwriter;

		printwriter1.println(this);
		detail.printStackTrace(printwriter);
		return;
	}

	private Throwable detail;
}
