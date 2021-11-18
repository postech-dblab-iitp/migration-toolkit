/*
 * Copyright (C) 2013 Search Solution Corporation. NONE rights reserved by Search
 * Solution.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met: -
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer. - Redistributions in binary
 * form must reproduce the above copyright notice, this list of conditions and
 * the following disclaimer in the documentation and/or other materials provided
 * with the distribution. - Neither the name of the <ORGANIZATION> nor the names
 * of its contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHNONE THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * 
 */
package com.cubrid.common.ui.common.notice;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.eclipse.core.runtime.Platform;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Text;
import org.eclipse.ui.PartInitException;
import org.eclipse.ui.PlatformUI;
import org.eclipse.ui.browser.IWebBrowser;
import org.eclipse.ui.browser.IWorkbenchBrowserSupport;
import org.eclipse.ui.forms.IManagedForm;
import org.eclipse.ui.forms.editor.FormEditor;
import org.eclipse.ui.forms.editor.FormPage;
import org.eclipse.ui.forms.events.HyperlinkEvent;
import org.eclipse.ui.forms.events.IHyperlinkListener;
import org.eclipse.ui.forms.widgets.FormText;
import org.eclipse.ui.forms.widgets.FormToolkit;
import org.eclipse.ui.forms.widgets.ScrolledForm;
import org.eclipse.ui.forms.widgets.Section;
import org.eclipse.ui.forms.widgets.TableWrapData;
import org.eclipse.ui.forms.widgets.TableWrapLayout;

/**
 * CM Dashboard Page
 * 
 * @author Tobi
 * 
 * @version 1.0
 * @date 2013-1-28
 */
public class NoticeDashboardPage extends
		FormPage {

	private static final String rsssource = "https://cubrid.github.io/";
	private static String language = Platform.getNL();
	static {
		if (language.equals("ko_KR")) {
			language = "ko";
		} else {
			language = "en";
		}
	}

	private String client = ApplicationType.CUBRID_MANAGER.getRssName();
	private FormToolkit toolkit;
	private ScrolledForm form;
	private Composite left, right;
	private int readRssStatus = RssStatusCode.SUCCESS;
	private NoticeDashboardEntity rssEntity;
	private List<Section> sections = new ArrayList<Section>();
	private Map<String, FormText> formTexts = new HashMap<String, FormText>();

	public NoticeDashboardPage(FormEditor editor, String id, String title,
			String client) {
		super(editor, id, title);
		this.client = client;
	}

	public void dispose() {
		if (form != null && !form.isDisposed()) {
			Font font = form.getFont();
			if (font != null && !font.isDisposed()) {
				font.dispose();
			}
		}
		super.dispose();
	}

	protected void createFormContent(IManagedForm managedForm) {
		super.createFormContent(managedForm);
		toolkit = managedForm.getToolkit();
		form = managedForm.getForm();

		toolkit.decorateFormHeading(form.getForm());
		String title = client.equals("CMT") ? Messages.titleCUBRIDMigration
				: client.equals("CQB") ? Messages.titleCUBRIDQuery
						: Messages.titleCUBRIDManager;
		form.setText(title);
		form.setFont(new Font(Display.getCurrent(), "Arial", 13, SWT.BOLD));

		Composite composite = form.getBody();
		TableWrapLayout layout2 = new TableWrapLayout();
		layout2.numColumns = 2;
		layout2.makeColumnsEqualWidth = true;
		composite.setLayout(layout2);

		TableWrapLayout layout1 = new TableWrapLayout();
		layout1.numColumns = 1;
		TableWrapData twd;

		left = toolkit.createComposite(composite, SWT.None);
		left.setLayout(layout1);
		twd = new TableWrapData(TableWrapData.FILL_GRAB);
		//twd.maxWidth = 10;
		left.setLayoutData(twd);

		layout1 = new TableWrapLayout();
		layout1.numColumns = 1;
		right = toolkit.createComposite(composite, SWT.None);
		right.setLayout(layout1);
		twd = new TableWrapData(TableWrapData.FILL_GRAB);
		right.setLayoutData(twd);

		createContentSections(left, right);

		ScheduledExecutorService pool = Executors.newSingleThreadScheduledExecutor();
		pool.schedule(new Runnable() {
			public void run() {
				initData();
				Display.getDefault().syncExec(new Runnable() {
					public void run() {
						setContent();
					}
				});
			}
		}, 10, TimeUnit.MILLISECONDS);
		pool.shutdown();
	}

	private void setContent() {

		// Hide it
		// createActionSection(left);
		if (readRssStatus == RssStatusCode.SUCCESS) {
			for (Entry<String, FormText> entry : formTexts.entrySet()) {
				String content = rssEntity.getHtmlContent(entry.getKey(),
						client);
				try {
					entry.getValue().setText(content, true, false);
				} catch (Exception e) {
					entry.getValue().setText(e.getMessage(), false, false);
				}
			}
			for (Section section : sections) {
				section.setExpanded(true);
			}
		} else if (readRssStatus == RssStatusCode.SUCCESS_GET_FROM_CACHE) {
			for (Entry<String, FormText> entry : formTexts.entrySet()) {
				String content = rssEntity.getHtmlContent(entry.getKey(),
						client);
				try {
					entry.getValue().setText(content, true, false);
				} catch (Exception e) {
					entry.getValue().setText(e.getMessage(), false, false);
				}
			}
			for (Section section : sections) {
				section.setExpanded(true);
			}
		} else {
			Text msgText = new Text(left, SWT.NONE);
			msgText.setText(Messages.networkConnectionError);
			for (Section section : sections) {
				section.dispose();
			}
			left.pack();
		}
	}

	private void initData() {
		String fileName = "cubridtools_" + language + ".xml";
		rssEntity = new NoticeDashboardEntity(rsssource + fileName);
		readRssStatus = rssEntity.refresh();
	}

	@SuppressWarnings("unused")
	private void createActionSection(Composite parent) {
		//		Section section = createStaticSection(parent, Messages.titleCommonAction);
		//
		//		Composite composite = toolkit.createComposite(section);
		//		GridLayout gridLayout = new GridLayout();
		//		gridLayout.numColumns = 2;
		//		composite.setLayout(gridLayout);
		//		composite.setLayoutData(new GridData(GridData.FILL_BOTH));
		//		section.setClient(composite);
		//
		//		GridData data = new GridData(GridData.BEGINNING);
		//		data.widthHint = 200;
		//
		//		ImageHyperlink addHost = toolkit.createImageHyperlink(composite, SWT.NONE);
		//		addHost.setLayoutData(data);
		//		addHost.setImage(MigrationUIPlugin.getImageDescriptor("icons/action/host_add.png").createImage());
		//		addHost.setText(Messages.titleAddHostBtn);
		//		addHost.addHyperlinkListener(new IHyperlinkListener() {
		//			public void linkExited(HyperlinkEvent e) {
		//			}
		//
		//			public void linkEntered(HyperlinkEvent e) {
		//			}
		//
		//			public void linkActivated(HyperlinkEvent e) {
		//				ActionManager.getInstance().getAction("com.cubrid.cubridmanager.ui.host.action.AddHostAction").run();
		//			}
		//		});
		//
		//		ImageHyperlink connectHost = toolkit.createImageHyperlink(composite, SWT.NONE);
		//		connectHost.setLayoutData(data);
		//		connectHost.setImage(MigrationUIPlugin.getImageDescriptor("icons/action/host_connect.png").createImage());
		//		connectHost.setText(Messages.titleConHostBtn);
		//		connectHost.addHyperlinkListener(new IHyperlinkListener() {
		//			public void linkExited(HyperlinkEvent e) {
		//			}
		//
		//			public void linkEntered(HyperlinkEvent e) {
		//			}
		//
		//			public void linkActivated(HyperlinkEvent e) {
		//				ActionManager.getInstance().getAction("com.cubrid.cubridmanager.ui.host.action.ConnectHostAction").run();
		//			}
		//		});
		//
		//		ImageHyperlink preferences = toolkit.createImageHyperlink(composite, SWT.NONE);
		//		preferences.setLayoutData(data);
		//		preferences.setImage(MigrationUIPlugin.getImageDescriptor("icons/action/property.png").createImage());
		//		preferences.setText(Messages.titlePreferencesBtn);
		//		preferences.addHyperlinkListener(new IHyperlinkListener() {
		//			public void linkExited(HyperlinkEvent e) {
		//			}
		//
		//			public void linkEntered(HyperlinkEvent e) {
		//			}
		//
		//			public void linkActivated(HyperlinkEvent e) {
		//				ActionManager.getInstance().getAction("preferences").run();
		//			}
		//		});
		//
		//		ImageHyperlink help = toolkit.createImageHyperlink(composite, SWT.NONE);
		//		help.setLayoutData(data);
		//		help.setImage(MigrationUIPlugin.getImageDescriptor("icons/action/help.png").createImage());
		//		help.setText(Messages.titleHelpBtn);
		//		help.addHyperlinkListener(new IHyperlinkListener() {
		//			public void linkExited(HyperlinkEvent e) {
		//			}
		//
		//			public void linkEntered(HyperlinkEvent e) {
		//			}
		//
		//			public void linkActivated(HyperlinkEvent e) {
		//				ActionManager.getInstance().getAction(HelpDocumentAction.ID).run();
		//			}
		//		});
	}

	private void createContentSections(Composite left, Composite right) {
		Section section;

		section = createExpandableSection(left, Messages.titleAnnouncement,
				"Announcement");
		section.setExpanded(true);

		section = createExpandableSection(left, Messages.titleMajorFeatures,
				"MajorFeatures");
		section.setExpanded(true);

		section = createExpandableSection(left, Messages.titleTechTrends,
				"TechTrends");
		section.setExpanded(true);

		section = createExpandableSection(right, Messages.titleReleaseNews,
				"ReleaseNews");
		section.setExpanded(true);

		section = createExpandableSection(right, Messages.titleTutorials,
				"Tutorials");
		section.setExpanded(true);

		//		section = createExpandableSection(right, Messages.titleHowStart, "HowStart");
		//		section.setExpanded(true);

		section = createExpandableSection(right, Messages.titleUsefulLinks,
				"UsefulLinks");
		section.setExpanded(true);

	}

	//	private Section createStaticSection(Composite parent, String title) {
	//		return createSection(parent, title, null, Section.TITLE_BAR);
	//	}

	@SuppressWarnings("unused")
	private Section createStaticSection(Composite parent, String title,
			String category) {
		return createSection(parent, title, category, Section.TITLE_BAR);
	}

	private Section createExpandableSection(Composite parent, String title,
			String category) {
		return createSection(parent, title, category, Section.TITLE_BAR
				| Section.TWISTIE);
	}

	private Section createSection(Composite parent, String title,
			String category, int sectionStyle) {
		Section section = toolkit.createSection(parent, sectionStyle);
		section.clientVerticalSpacing = 5;
		section.setLayout(new GridLayout());
		section.setLayoutData(new TableWrapData(TableWrapData.FILL_GRAB));
		section.setText(title);

		if (category != null) {
			TableWrapLayout layout = new TableWrapLayout();
			layout.numColumns = 1;

			Composite composite = toolkit.createComposite(section, SWT.None);
			composite.setLayout(layout);
			composite.setLayoutData(new GridData(GridData.FILL_BOTH));

			section.setClient(composite);

			FormText text = createFormText(composite);
			formTexts.put(category, text);
		}

		sections.add(section);
		return section;
	}

	private FormText createFormText(Composite composite) {
		FormText text = toolkit.createFormText(composite, true);
		text.addHyperlinkListener(new IHyperlinkListener() {
			public void linkExited(HyperlinkEvent e) {
			}

			public void linkEntered(HyperlinkEvent e) {
			}

			public void linkActivated(HyperlinkEvent e) {
				String href = (String) e.getHref();
				String url = urlEncodeForSpaces(href);
				try {
					IWorkbenchBrowserSupport support = PlatformUI.getWorkbench().getBrowserSupport();
					IWebBrowser browser = support.getExternalBrowser();
					browser.openURL(new URL(url));
				} catch (PartInitException e1) {
					e1.printStackTrace();
				} catch (MalformedURLException e2) {
					e2.printStackTrace();
				}
			}
		});
		return text;
	}

	/**
	 * This method encodes the url, removes the spaces from the url and replaces
	 * the same with <code>"%20"</code>.
	 * 
	 * @param input the input char array
	 * @return the string
	 */
	public static String urlEncodeForSpaces(char[] input) {
		StringBuffer retu = new StringBuffer(input.length);
		for (int i = 0; i < input.length; i++) {
			if (input[i] == ' ') {
				retu.append("%20");
			} else {
				retu.append(input[i]);
			}
		}
		return retu.toString();
	}

	/**
	 * This method encodes the url, removes the spaces from the url and replaces
	 * the same with <code>"%20"</code>.
	 * 
	 * @param input the input string
	 * @return the string
	 */
	public static String urlEncodeForSpaces(String input) {
		return urlEncodeForSpaces(input.toCharArray());
	}
}
