/*
 * Copyright (C) 2009 Search Solution Corporation. All rights reserved by Search Solution. 
 *
 * Redistribution and use in source and binary forms, with or without modification, 
 * are permitted provided that the following conditions are met: 
 *
 * - Redistributions of source code must retain the above copyright notice, 
 *   this list of conditions and the following disclaimer. 
 *
 * - Redistributions in binary form must reproduce the above copyright notice, 
 *   this list of conditions and the following disclaimer in the documentation 
 *   and/or other materials provided with the distribution. 
 *
 * - Neither the name of the <ORGANIZATION> nor the names of its contributors 
 *   may be used to endorse or promote products derived from this software without 
 *   specific prior written permission. 
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND 
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. 
 * IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, 
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, 
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, 
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY 
 * OF SUCH DAMAGE. 
 *
 */
package com.cubrid.cubridmigration.core.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;

/**
 * 
 * SSH utilities
 * 
 * @author Kevin Cao
 * 
 */
public class SSHUtils {

	private static final int CON_TIME_OUT = 20000;
	private static final Logger LOG = LogUtil.getLogger(SSHUtils.class);

	/**
	 * check remote input stream returned
	 * 
	 * @param in input stream
	 * @param errorMsg store error messages.
	 * @return result code
	 * @throws IOException ex
	 */
	private static int checkAck(InputStream in, StringBuffer errorMsg) throws IOException {
		int result = in.read();
		// result may be 0 for success,
		if (result == 0) {
			return result;
		}

		// -1
		if (result == -1) {
			return result;
		}

		// 1 for error,
		// 2 for fatal error,
		if (result == 1 || result == 2) {
			int c;
			do {
				c = in.read();
				errorMsg.append((char) c);
			} while (c != '\n');
		}
		return result;
	}

	/**
	 * Create and connect a SSH session with SSHHost instance. If proxy is
	 * enabled, the main host's authentication's type is password only.
	 * 
	 * @param host SSHHost
	 * @return connected session or null
	 */
	public static Session newSSHSession(SSHHost host) {
		try {
			Session session;
			if (host.isUseProxy() && host.getProxy() != null) {
				Session gatewaySession = createSession(host.getProxy());
				if (gatewaySession == null) {
					return null;
				}
				gatewaySession.connect(CON_TIME_OUT);
				host.setAuthType(0);
				session = createSession(host);
				if (session != null) {
					session.setProxy(new ProxySSH(gatewaySession));
				}
			} else {
				session = createSession(host);
			}
			if (session != null) {
				session.connect(CON_TIME_OUT);
			}
			return session;
		} catch (Exception e) {
			LOG.error("Connect SSH server failed", e);
			throw new SSHConnectFailedException(e.getMessage(), e);
		}
	}

	/**
	 * Create a session without connected, just configured.
	 * 
	 * @param host SSHHostBaseInfo
	 * @return Session or null
	 */
	private static Session createSession(SSHHostBaseInfo host) {
		try {
			JSch jsch = new JSch();
			if (host.getAuthType() == 1) {
				jsch.addIdentity(host.getPrivateKeyAbsoluteFile());
			}
			Session session = jsch.getSession(host.getUser(), host.getHost(), host.getPort());
			session.setConfig("StrictHostKeyChecking", "no");
			if (host.getAuthType() == 0) {
				session.setConfig("PreferredAuthentications", "password");
				UserInfo ui = new RemoteServerUserInfo(host.getPassword(), true);
				session.setUserInfo(ui);
				session.setPassword(host.getPassword());
			} else if (host.getAuthType() == 1) {
				UserInfo ui = new RemoteServerUserInfo(host.getPassword(), false);
				session.setUserInfo(ui);
			} else {
				initKRBEnvironment(host);
				session.setConfig("PreferredAuthentications", "gssapi-with-mic");
				session.setUserInfo(new BaseJSCHUser());
				session.setPassword(host.getPassword());
			}
			return session;
		} catch (Exception e) {
			LOG.error("Connect SSH server failed", e);
		}
		return null;
	}

	/**
	 * Initialize the kerberos authentication's environment of local.
	 * 
	 * @param host SSHHostBaseInfo
	 */
	private static void initKRBEnvironment(SSHHostBaseInfo host) {
		if (host.getAuthType() != 2) {
			return;
		}
		try {
			//Initialize kerveros authentication
			System.setProperty("java.security.krb5.conf", host.getKrbConfig());
			String gssLoginFile = host.getKrbTicket();
			System.setProperty("java.security.auth.login.config", gssLoginFile);
			System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
			File gssFile = new File(gssLoginFile);
			if (!gssFile.exists()) {
				String content = CommonUtils.getGSSLoginConfigContent(PathUtils.getDefaultTicketFile());
				CUBRIDIOUtils.writeLines(gssFile, new String[] {content});
			}
		} catch (Exception ex) {
			LOG.error(ex);
		}
	}

	/**
	 * Scp from SSH server
	 * 
	 * @param session Session
	 * @param rfile remote file name with full path
	 * @param lfile local file name
	 * @return error message
	 * @throws Exception ex
	 */
	public static String scpFrom(Session session, String rfile, String lfile) throws Exception {

		String prefix = null;
		if (new File(lfile).isDirectory()) {
			prefix = lfile + File.separator;
		}

		// exec 'scp -f rfile' remotely
		String command = "scp -f " + rfile;
		Channel channel = session.openChannel("exec");
		((ChannelExec) channel).setCommand(command);

		// get I/O streams for remote scp
		OutputStream out = channel.getOutputStream();
		InputStream in = channel.getInputStream();

		channel.connect();

		byte[] buf = new byte[1024];

		// send '\0'
		buf[0] = 0;
		out.write(buf, 0, 1);
		out.flush();

		StringBuffer errorMsg = new StringBuffer();
		while (true) {
			int c = SSHUtils.checkAck(in, errorMsg);
			if (c != 'C') {
				break;
			}
			// read '0644 '
			in.read(buf, 0, 5);

			long filesize = 0L;
			while (true) {
				if (in.read(buf, 0, 1) < 0) {
					// error
					break;
				}
				if (buf[0] == ' ') {
					break;
				}

				filesize = filesize * 10L + (long) (buf[0] - '0');
			}

			String file = null;
			for (int i = 0;; i++) {
				in.read(buf, i, 1);
				if (buf[i] == (byte) 0x0a) {
					file = new String(buf, 0, i);
					break;
				}
			}
			// send '\0'
			buf[0] = 0;
			out.write(buf, 0, 1);
			out.flush();

			// read a content of lfile
			FileOutputStream fos = null;
			try {
				lfile = prefix == null ? lfile : prefix + file;
				fos = new FileOutputStream(lfile);
				int foo;
				while (true) {
					if (buf.length < filesize) {
						foo = buf.length;
					} else {
						foo = (int) filesize;
					}
					foo = in.read(buf, 0, foo);
					if (foo < 0) {
						// error
						break;
					}
					fos.write(buf, 0, foo);
					filesize -= foo;
					if (filesize == 0L) {
						break;
					}
				}
				fos.close();
				fos = null;
			} finally {
				if (fos != null) {
					fos.close();
				}
			}
			if (SSHUtils.checkAck(in, errorMsg) != 0) {
				return errorMsg.toString();
			}
			// send '\0'
			buf[0] = 0;
			out.write(buf, 0, 1);
			out.flush();
		}
		channel.disconnect();
		return errorMsg.toString();
	}

	/**
	 * Scp to SSH server
	 * 
	 * @param session Session
	 * @param localFile local file name
	 * @param remoteFile remote file name with full path
	 * @return error messages
	 * @throws Exception ex
	 */
	public static String scpTo(Session session, String localFile, String remoteFile) throws Exception {
		FileInputStream fis = null;
		ChannelExec channel = null;
		try {
			// exec 'scp -t rfile' remotely
			String command1 = "scp -P " + session.getPort() + " -t " + remoteFile;
			channel = (ChannelExec) session.openChannel("exec");
			channel.setCommand(command1);

			// get I/O streams for remote scp
			OutputStream out = channel.getOutputStream();
			InputStream in = channel.getInputStream();

			channel.connect();
			StringBuffer errorMsg = new StringBuffer();
			if (SSHUtils.checkAck(in, errorMsg) != 0) {
				return errorMsg.toString();
			}

			File lFile = new File(localFile);

			// send "C0644 filesize filename", where filename should not include
			// '/'
			long filesize = lFile.length();
			String command2 = "C0644 " + filesize + " ";
			if (localFile.lastIndexOf('/') > 0) {
				command2 += localFile.substring(localFile.lastIndexOf('/') + 1);
			} else {
				command2 += localFile;
			}
			command2 += "\n";
			out.write(command2.getBytes());
			out.flush();
			if (SSHUtils.checkAck(in, errorMsg) != 0) {
				return errorMsg.toString();
			}

			// send a content of lfile
			fis = new FileInputStream(localFile);
			byte[] buf = new byte[1024];
			while (true) {
				int len = fis.read(buf, 0, buf.length);
				if (len <= 0) {
					break;
				}
				out.write(buf, 0, len);
				out.flush();
			}
			// send '\0'
			buf[0] = 0;
			out.write(buf, 0, 1);
			out.flush();
			SSHUtils.checkAck(in, errorMsg);
			return errorMsg.toString();
		} finally {
			if (fis != null) {
				fis.close();
			}
			if (channel != null && channel.isConnected()) {
				channel.disconnect();
			}
		}
	}
}