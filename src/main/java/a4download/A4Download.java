package a4download;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import arc.mf.client.RemoteServer;
import arc.mf.client.ServerClient;
import arc.streams.StreamCopy;
import arc.xml.XmlDoc;
import arc.xml.XmlStringWriter;

public class A4Download {
    public static final int STUDY_CID_DEPTH = 6;
    public static final int DATASET_CID_DEPTH = 7;
    public static final String DEFAULT_HOST = "daris-1.cloud.unimelb.edu.au";
    public static final int DEFAULT_PORT = 443;
    public static final String DEFAULT_PROTOCOL = "https";
    public static final String DEFAULT_TOKEN = null;

    public static void main(String[] args) throws Throwable {
        File outDir = new File(
                new File("").getAbsoluteFile().getAbsolutePath());
        String host = DEFAULT_HOST;
        int port = DEFAULT_PORT;
        String protocol = DEFAULT_PROTOCOL;
        String token = DEFAULT_TOKEN;
        boolean deleteWorklistFileAfterDownload = false;
        List<File> patientWorklistFiles = new ArrayList<File>();
        try {
            for (int i = 0; i < args.length;) {
                String arg = args[i];
                if (arg.equals("-h")) {
                    printUsage();
                    System.exit(0);
                } else if (arg.equals("-d")) {
                    deleteWorklistFileAfterDownload = true;
                    i++;
                } else if (arg.equals("-o")) {
                    outDir = new File(args[i + 1]);
                    i += 2;
                } else if (arg.equals("--host")) {
                    host = args[i + 1];
                    i += 2;
                } else if (arg.equals("--port")) {
                    port = Integer.parseInt(args[i + 1]);
                    i += 2;
                } else if (arg.equals("--protocol")) {
                    protocol = args[i + 1];
                    if (!"http".equalsIgnoreCase(protocol)
                            && !"https".equalsIgnoreCase(protocol)
                            && !"tcp/ip".equalsIgnoreCase(protocol)) {
                        throw new IllegalArgumentException(
                                "Invalid --protocol: " + protocol);
                    }
                    i += 2;
                } else if (arg.equals("--token")) {
                    token = args[i + 1];
                    i += 2;
                } else {
                    File pwf = new File(arg);
                    if (!pwf.exists()) {
                        throw new FileNotFoundException(
                                "File " + pwf + " is not found.");
                    }
                    patientWorklistFiles.add(pwf);
                    i++;
                }
            }
            if (host == null) {
                throw new Exception("No Mediaflux server host is specified.");
            }
            if (port <= 0) {
                throw new Exception("Invalid Mediaflux server port: " + port);
            }
            if (protocol == null) {
                throw new Exception(
                        "No Mediaflux server protocol is specified.");
            } else if (!(protocol.equalsIgnoreCase("http")
                    || protocol.equalsIgnoreCase("https")
                    || protocol.equalsIgnoreCase("tcp/ip"))) {
                throw new Exception("Invalid Mediaflux protocol: " + protocol);
            }
            if (patientWorklistFiles.isEmpty()) {
                throw new Exception("No patient worklist file is specified.");
            }
            RemoteServer server = new RemoteServer(host, port,
                    protocol.toLowerCase().startsWith("http"),
                    protocol.equalsIgnoreCase("https"));
            ServerClient.Connection cxn = server.open();
            try {
                cxn.connectWithToken(token);
                download(cxn, patientWorklistFiles, outDir,
                        deleteWorklistFileAfterDownload);
            } finally {
                cxn.close();
            }
        } catch (Throwable e) {
            System.err.println("Error: " + e.getMessage());
            printUsage();
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.out.println(
                "Usage: a4download [options] [-o out-dir] [-d] worklist-files");
        System.out.println("Options:");
        System.out.println(
                "       --host <mflux-host>         The mediaflux server host.");
        System.out.println(
                "       --port <mflux-port>         The mediaflux server port.");
        System.out.println(
                "       --protocol <mflux-protocol> The mediaflux server protocol.");
        System.out.println(
                "       --token <secure-token>      The secure identity token to authenticate with Mediaflux server.");
        System.out.println(
                "       -o <output-dir>             The output directory. If not specified, defaults to current directory.");
        System.out.println(
                "       -d                          Delete the input worklist txt file after downloading. If not specified, the worklist file will not be deleted.");
    }

    private static void download(ServerClient.Connection cxn,
            List<File> patientWorklistFiles, File outDir,
            boolean deleteWorklistFileAfterDownload) throws Throwable {
        if (patientWorklistFiles != null) {
            for (File f : patientWorklistFiles) {
                download(cxn, f, outDir, deleteWorklistFileAfterDownload);
            }
        }
    }

    private static void download(ServerClient.Connection cxn,
            File patientWorklistFile, File outDir,
            boolean deleteWorklistFileAfterDownload) throws Throwable {
        String cid = null;
        String a4id = null;
        String petSeriesDescription = null;
        String ctSeriesDescription = null;
        System.out.println("Parsing patient work-list file: "
                + patientWorklistFile.getAbsolutePath() + "...");
        BufferedInputStream bis = new BufferedInputStream(
                new FileInputStream(patientWorklistFile));
        bis.mark(2);
        int c1 = bis.read();
        int c2 = bis.read();
        bis.reset();
        String encoding = "UTF-8";
        if (c1 == 0xff && (c2 == 0xff || c2 == 0xfe)) {
            encoding = "UTF-16";
        }
        BufferedReader br = new BufferedReader(
                new InputStreamReader(bis, encoding));
        try {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("DARIS_ID:")) {
                    cid = line.trim().substring(9);
                    int depth = getCidDepth(cid);
                    if (depth == STUDY_CID_DEPTH) {
                        // OK
                    } else if (depth == DATASET_CID_DEPTH) {
                        cid = getParentCid(cid);
                    } else {
                        throw new Exception("Invalid DaRIS citeable id: " + cid
                                + ". It must be a study id.");
                    }
                } else if (line.startsWith("NEWID:")) {
                    a4id = line.trim().substring(6);
                    if (!a4id.startsWith("a4") && !a4id.startsWith("A4")) {
                        a4id = "A4" + a4id;
                    }
                } else if (line.startsWith("CTFILETSEND:")) {
                    ctSeriesDescription = line.trim().substring(12);
                } else if (line.startsWith("PETFILETSEND:")) {
                    petSeriesDescription = line.trim().substring(13);
                }
            }
        } finally {
            br.close();
            bis.close();
        }
        if (cid == null) {
            throw new Exception("No DARIS_ID found in file "
                    + patientWorklistFile.getAbsolutePath() + ".");
        }
        if (a4id == null) {
            throw new Exception("No NEWID found in file "
                    + patientWorklistFile.getAbsolutePath() + ".");
        }
        if (ctSeriesDescription == null) {
            throw new Exception("No CTFILETSEND found in file "
                    + patientWorklistFile.getAbsolutePath() + ".");
        }
        if (petSeriesDescription == null) {
            throw new Exception("No PETFILETSEND found in file "
                    + patientWorklistFile.getAbsolutePath() + ".");
        }
        download(cxn, cid, petSeriesDescription, ctSeriesDescription, a4id,
                outDir);

        if (deleteWorklistFileAfterDownload) {
            System.out.print("Deleting input worklist file: "
                    + patientWorklistFile.getAbsolutePath());
            if (!patientWorklistFile.delete()) {
                patientWorklistFile.deleteOnExit();
            }
            System.out.println("deleted.");
        }
    }

    private static File download(ServerClient.Connection cxn, String studyCid,
            String petSeriesDescription, String ctSeriesDescription,
            String a4id, File dir) throws Throwable {
        Map<Integer, String> override = new TreeMap<Integer, String>();
        override.put(0x00100010, a4id + "^" + a4id); // patient name
        override.put(0x00100020, a4id); // patient id
        override.put(0x0008103e, a4id); // series description
        // make directory for the patient/subject
        File subDir = new File(dir, a4id);
        if (!subDir.exists()) {
            subDir.mkdir();
        }

        /*
         * ct
         */
        System.out
                .println("Looking for CT series in study " + studyCid + "...");
        String ctCid = findDicomSeries(cxn, studyCid, ctSeriesDescription);
        System.out.println("Found CT series: " + ctCid);
        // make sub-directory for ct series
        File ctDir = new File(subDir, "ct");
        if (!ctDir.exists()) {
            ctDir.mkdir();
        }
        System.out.println("Downloading CT series " + ctCid + "...");
        File ctZipFile = downloadDicomSeries(cxn, ctCid, override, dir);
        System.out.println(
                "Downloaded CT series: " + ctZipFile.getAbsolutePath() + ".");
        // unzip to ${a4id}/ct directory
        unzip(new ZipFile(ctZipFile), ctDir);
        System.out.println(
                "Extracting CT series to " + ctDir.getAbsolutePath() + "...");
        if (!ctZipFile.delete()) {
            ctZipFile.deleteOnExit();
        }

        /*
         * 
         */
        System.out
                .println("Looking for PET series in study " + studyCid + "...");
        String petCid = findDicomSeries(cxn, studyCid, petSeriesDescription);
        System.out.println("Found PET series: " + petCid);
        // make sub-directory for pet series
        File petDir = new File(subDir, "pet");
        if (!petDir.exists()) {
            petDir.mkdir();
        }
        System.out.println("Downloading PET series " + petCid + "...");
        File petZipFile = downloadDicomSeries(cxn, petCid, override, dir);
        System.out.println(
                "Downloaded PET series: " + petZipFile.getAbsolutePath() + ".");
        // unzip to ${a4id}/pet directory
        System.out.println(
                "Extracting PET series to " + petDir.getAbsolutePath() + "...");
        unzip(new ZipFile(petZipFile), petDir);
        if (!petZipFile.delete()) {
            petZipFile.deleteOnExit();
        }

        // make the final zip
        File zipFile = new File(dir, a4id + ".zip");
        ZipOutputStream zo = null;
        try {
            zo = new ZipOutputStream(
                    new BufferedOutputStream(new FileOutputStream(zipFile)));
            System.out.println("Adding PET DICOM files to "
                    + zipFile.getAbsolutePath() + "...");
            zipDirectory(petDir, zo);
            System.out.println("Adding CT DICOM files to "
                    + zipFile.getAbsolutePath() + "...");
            zipDirectory(ctDir, zo);
        } finally {
            if (zo != null) {
                zo.close();
            }
        }

        System.out.println(
                "Deleting directory " + subDir.getAbsolutePath() + "...");
        delete(subDir);

        System.out.println("Downloaded " + zipFile.getAbsolutePath() + ".");

        return zipFile;
    }

    private static void delete(File f) {
        if (f.isDirectory()) {
            File[] cfs = f.listFiles();
            for (File cf : cfs) {
                delete(cf);
            }
        }
        if (!f.delete()) {
            f.deleteOnExit();
        }
    }

    private static File downloadDicomSeries(ServerClient.Connection cxn,
            String cid, Map<Integer, String> override, File dir)
                    throws Throwable {
        XmlStringWriter w = new XmlStringWriter();
        w.add("atype", "zip");
        w.add("cid", cid);
        if (override != null && !override.isEmpty()) {
            w.push("override");
            for (int tag : override.keySet()) {
                String value = override.get(tag);
                String group = String.format("%04x", tag >>> 16);
                String element = String.format("%04x", tag & 0xffff);
                if (value == null || value.trim().equals("")) {
                    w.push("element", new String[] { "group", group, "element",
                            element, "anonymize", Boolean.toString(true) });
                } else {
                    w.push("element", new String[] { "group", group, "element",
                            element });
                    w.add("value", value);
                }
                w.pop();
            }
            w.pop();
        }
        File zipFile = new File(dir, cid + ".zip");
        ServerClient.Output output = new ServerClient.FileOutput(zipFile);
        try {
            cxn.execute("daris.dicom.download", w.document(), null, output);
        } finally {
            output.close();
        }
        return zipFile;
    }

    private static String findDicomSeries(ServerClient.Connection cxn,
            String studyCid, String seriesDescription) throws Throwable {
        XmlStringWriter w = new XmlStringWriter();
        w.add("where",
                "cid in '" + studyCid
                        + "' and xpath(mf-dicom-series/description)='"
                        + seriesDescription + "'");
        w.add("action", "get-cid");
        XmlDoc.Element re = cxn.execute("asset.query", w.document());
        if (re.count("cid") < 1) {
            throw new Exception("No dicom series with description: '"
                    + seriesDescription + "' found in study " + studyCid + ".");
        }
        if (re.count("cid") > 1) {
            throw new Exception(
                    "Found more than one dicom series with description: '"
                            + seriesDescription + "' in study " + studyCid
                            + ".");
        }
        return re.value("cid");
    }

    private static void unzip(ZipFile zipFile, File outDir) throws Throwable {
        Enumeration<? extends ZipEntry> entries = zipFile.entries();
        while (entries.hasMoreElements()) {
            ZipEntry entry = entries.nextElement();
            InputStream is = null;
            File of = new File(outDir, entry.getName());
            try {
                is = new BufferedInputStream(zipFile.getInputStream(entry));
                StreamCopy.copy(is, of);
            } finally {
                if (is != null) {
                    is.close();
                }
            }
        }
    }

    private static void zipDirectory(File dir, ZipOutputStream zo)
            throws Throwable {
        File[] files = dir.listFiles(new FileFilter() {
            public boolean accept(File file) {
                return file.isFile();
            }
        });
        for (File f : files) {
            String entryName = dir.getName() + File.separator + f.getName();
            ZipEntry entry = new ZipEntry(entryName);
            zo.putNextEntry(entry);
            InputStream is = new BufferedInputStream(new FileInputStream(f));
            try {
                StreamCopy.copy(is, zo);
            } finally {
                is.close();
            }
        }
    }

    private static String getParentCid(String cid) {
        int idx = cid.lastIndexOf('.');
        if (idx == -1) {
            return cid;
        } else {
            return cid.substring(0, idx);
        }
    }

    private static int getCidDepth(String cid) {
        return cid.split("\\.").length;
    }

}
