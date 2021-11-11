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

/* -*-mode:java; c-basic-offset:2; indent-tabs-mode:nil -*- */

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import com.jcraft.jsch.ChannelDirectTCPIP;
import com.jcraft.jsch.Proxy;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SocketFactory;

/**
 * A Proxy implementation using an JSch Session to a gateway node as the tunnel.
 * The Session will not be closed on close of the Proxy, only the tunneling
 * channel.
 * 
 * @author Paŭlo Ebermann, with some hints from ymnk.
 */
public class ProxySSH implements
		Proxy {

	public ProxySSH(Session gateway) {
		this.gateway = gateway;
	}

	private Session gateway;
	private ChannelDirectTCPIP channel;
	private InputStream iStream;
	private OutputStream oStream;

	/**
	 * closes the socket + streams.
	 */
	public void close() {
		channel.disconnect();
	}

	/**
	 * connects to the remote server.
	 * 
	 * @param ignore the socket factory. This is not used.
	 * @param host the remote host to use.
	 * @param port the port number to use.
	 * @param timeout the timeout for connecting. (TODO: This is not used, for
	 *        now.)
	 * @throws Exception if there was some problem.
	 */
	public void connect(SocketFactory ignore, String host, int port, int timeout) throws Exception {
		channel = (ChannelDirectTCPIP) gateway.openChannel("direct-tcpip");
		channel.setHost(host);
		channel.setPort(port);
		// important: first create the streams, then connect.
		iStream = channel.getInputStream();
		oStream = channel.getOutputStream();
		channel.connect(timeout);
	}

	/**
	 * Returns an input stream to read data from the remote server.
	 * 
	 * @return InputStream
	 */
	public InputStream getInputStream() {
		return iStream;
	}

	public OutputStream getOutputStream() {
		return oStream;
	}

	public Socket getSocket() {
		// there is no socket.
		return null;
	}

}
