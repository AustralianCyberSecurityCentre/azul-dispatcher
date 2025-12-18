package identify

import (
	"log"
	"os"
	"testing"

	"github.com/goccy/go-json"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/testutils"
	"github.com/stretchr/testify/require"
)

var activeFileManager *testutils.FileManager

func TestMain(m *testing.M) {
	var err error
	activeFileManager, err = testutils.NewFileManager()
	if err != nil {
		log.Fatalf("Failed to create FileManager for us in identify tests with error: %v", err)
	}
	exitVal := m.Run()
	os.Exit(exitVal)
}

// Note - description is currently just to enforce documenting what binaries are.
func testDoIdentify(t *testing.T, sha256 string, description string, minimal bool) *events.BinaryEntityDatastream {
	identifier, err := NewIdentifier()
	require.Nil(t, err)
	defer identifier.Close()
	data, err := activeFileManager.DownloadFileBytes(sha256)
	require.Nil(t, err)

	// hash calculation
	h := NewHasher(minimal)
	err = h.Write(data)
	require.Nil(t, err)
	stream, err := h.Cook()
	require.Nil(t, err)

	if !minimal {
		// identify calculation
		testFile, err := os.CreateTemp("", "worker-test")
		require.Nil(t, err)
		_, err = testFile.Write(data)
		require.Nil(t, err)
		defer testFile.Close()
		defer os.Remove(testFile.Name())
		err = identifier.Identify(testFile.Name(), stream)
		require.Nil(t, err)
	}

	return stream
}

func testVerifyMetadata(t *testing.T, sha256, description, expected string) {
	ft := testDoIdentify(t, sha256, description, false)
	crushed, err := json.MarshalIndent(ft, "", "  ")
	require.Nil(t, err)
	nice := string(crushed)
	require.JSONEq(t, nice, expected, "expected\n%v", nice)
}
func testVerifyMetadataMinimal(t *testing.T, sha256, description, expected string) {
	ft := testDoIdentify(t, sha256, description, true)
	crushed, err := json.MarshalIndent(ft, "", "  ")
	require.Nil(t, err)
	nice := string(crushed)
	require.JSONEq(t, nice, expected, "expected\n%v", nice)
}

func testVerifyFiletype(t *testing.T, sha256, description, format, mime, magic string) {
	ft := testDoIdentify(t, sha256, description, false)
	if ft.FileFormatLegacy != format {
		t.Errorf("bad format: %v expected '%v' got '%v' | magic '%v'", sha256, format, ft.FileFormatLegacy, ft.Magic)
	}
	if ft.Mime != mime {
		t.Errorf("bad format: %v expected '%v' got '%v'", sha256, mime, ft.Mime)
	}
	if ft.Magic != magic {
		t.Errorf("bad format: %v expected '%v' got '%v'", sha256, magic, ft.Magic)
	}
}

func TestBasicWorker(t *testing.T) {
	testVerifyFiletype(t, "4d07389cc8be738474d240946a8094be4c89db028958a8e5b28290e4502ec8e2", "Benign text file containing the text 'This is some test text data.'",
		"Text", "text/plain", "ASCII text")
	testVerifyFiletype(t, "25ed3194250f3fc23145ca1d51e58b50004968d31223a89a7736b09377f7be47", "Benign text file containing the words 'This is some test text data.' for 795 lines.",
		"Text", "text/plain", "ASCII text")
	testVerifyFiletype(t, "992fa72f92e9b57ede1ba5e7e947f71019f60bde3a33d64d767c9e388f4a766b", "Benign password protected ARJ file.",
		"ARJ", "application/x-arj", "ARJ archive data, v11, password-protected (v1), slash-switched, created 17 dec 1980+43")
	testVerifyFiletype(t, "ffc7df1b29a93d861ad70ed3d5bccdfa4312140185520cc6a58cce5b9e11215a", "Benign auto open Macro document.",
		"MS Word Document", "application/msword", "Composite Document File V2 Document, Little Endian, Os: Windows, Version 5.1, Code page: 1252, Title: Test Document, Author: User01, Template: Normal.dot, Last Saved By: User1234, Revision Number: 7, Name of Creating Application: Microsoft Office Word, Total Editing Time: 32:00, Create Time/Date: Thu Mar 26 21:59:00 2015, Last Saved Time/Date: Thu Mar 26 22:45:00 2015, Number of Pages: 1, Number of Words: 16, Number of Characters: 95, Security: 0")
	testVerifyFiletype(t, "6e979eaf8a5b76ff14ec2784a5eac0ff509730bfebb351af37b8c9b6a6cc20e2", "Benign Open XML document.",
		"MS Word Document", "application/vnd.openxmlformats-officedocument.wordprocessingml.document", "Microsoft Word 2007+")
	testVerifyFiletype(t, "5f94858a80328bec92a0508ce3a9f4d4b088eb4f80a14569f856e7e01b72d642", "Benign Microsoft Word document, rc4.",
		"MS Word Document", "application/msword", "Composite Document File V2 Document, Little Endian, Os: Windows, Version 5.2, Code page: 1252, Author: DLEBLANC-DEV11B, Template: Normal.dot, Last Saved By: DLEBLANC-DEV11B, Revision Number: 1, Name of Creating Application: Microsoft Office Word, Total Editing Time: 08:00, Create Time/Date: Fri Feb  6 02:33:00 2009, Last Saved Time/Date: Fri Feb  6 02:41:00 2009, Number of Pages: 1, Number of Words: 0, Number of Characters: 0, Security: 1")
	testVerifyFiletype(t, "15c8614f493cf081b53ea379b06d759fc51cf94d61d245baab5efb77648bf8d4", "Benign RTF.",
		"Rich Text Format", "text/rtf", "Rich Text Format data, version 1, ANSI, code page 1252, default middle east language ID 1025")
	testVerifyFiletype(t, "cabb190a05e7381e07c42e37f01c1eec8b0c5323d5c5633c61e44df90d905c9e", "Malicious SYLK file with embedded command.",
		"SYLK", "application/x-sylk", "spreadsheet interchange document")
	testVerifyFiletype(t, "0dc315e0b3d9f4098ea5cac977b9814e3c6e9116cf296c1bbfcb3ab95c72ca99", "Malicious email (MIME HTML Archive), trojan, cve20120158.",
		"XML", "application/octet-stream", "data")
	testVerifyFiletype(t, "03bcc26e07e36ef89486bdb5d25a541916e1e85b5c959868ec069cf9745a9b92", "Benign SGML ASCII text.",
		"SGML", "text/plain", "exported SGML document, ASCII text")
}

func TestBasic(t *testing.T) {
	testVerifyMetadata(t, "4d07389cc8be738474d240946a8094be4c89db028958a8e5b28290e4502ec8e2",
		"Benign text file containing the text 'This is some test text data.'", `{
		"identify_version": 2,
		"label": "content",
		"size": 29,
		"sha512": "a89fbef47e4ef1b1af9916eb84996c1455402c68e2d12f2dceb15a66f31b149478bf9b2dc811e88ff93f032eb8fe26dc8ca3090b950eb83c87c6f13b5b15340a",
		"sha256": "4d07389cc8be738474d240946a8094be4c89db028958a8e5b28290e4502ec8e2",
		"sha1": "1d519120064f2d643b91caa1965d3d68089591bd",
		"md5": "a78166396aafd6c2f25bfa6f5eb25169",
		"mime": "text/plain",
		"magic": "ASCII text",
		"file_format_legacy": "Text",
		"file_format": "text/plain",
		"file_extension": "txt"
	  }`)
	testVerifyMetadata(t, "25ed3194250f3fc23145ca1d51e58b50004968d31223a89a7736b09377f7be47",
		"Benign text file containing the words 'This is some test text data.' for 795 lines.", ` {
		"identify_version": 2,
		"label": "content",
		"size": 23055,
		"sha512": "b2d4fd3aca6a8d3724ae64afd690657169c7fde4a2c01a8f232af3446dbe0bb8e93986c5989334aba06aba600489adbd74551366f9420fec888038ad8da5f3dc",
		"sha256": "25ed3194250f3fc23145ca1d51e58b50004968d31223a89a7736b09377f7be47",
		"sha1": "93887f57f13e4e080ac86d497d253a84dc98334b",
		"md5": "e52704d45ea4f8d3f5a57625f3504f75",
		"ssdeep": "12:hs66666666666666666666666666666666666666666666666666666666666662:o",
		"tlsh": "T167a2000c22e38238e0ec80c02ac030e8ef08e2cc0a2b8200b0c80c00280b82a20a0c08",
		"mime": "text/plain",
		"magic": "ASCII text",
		"file_format_legacy": "Text",
		"file_format": "text/plain",
		"file_extension": "txt"
	  }`)
	testVerifyMetadata(t, "992fa72f92e9b57ede1ba5e7e947f71019f60bde3a33d64d767c9e388f4a766b",
		"Benign password protected ARJ file.", `{
		"identify_version": 2,
		"label": "content",
		"size": 440,
		"sha512": "eb9dd9296917be15a9fcc461f12f926f270acc92db0909f112ae7fbcab23578eef8d758aae32251ebb9210a440223770ed990b66a20e6c51f635c0a7ca2dc4b9",
		"sha256": "992fa72f92e9b57ede1ba5e7e947f71019f60bde3a33d64d767c9e388f4a766b",
		"sha1": "c5984c97edd1f6ada9c55fb9bef8a5eaaf696c14",
		"md5": "07c80b56fa111ef5c6e84e3dbd08c4de",
		"tlsh":"T1e7f0abe318af8ea8e5a4e0329c52120c4fc6f1c1c496771c9a59cae58203adcbc6a017",
		"mime": "application/x-arj",
		"magic": "ARJ archive data, v11, password-protected (v1), slash-switched, created 17 dec 1980+43",
		"file_format_legacy": "ARJ",
		"file_format": "archive/arj",
		"file_extension": "arj"
	  }`)
	testVerifyMetadata(t, "15c8614f493cf081b53ea379b06d759fc51cf94d61d245baab5efb77648bf8d4",
		"Benign RTF.", `{
		"identify_version": 2,
		"label": "content",
		"size": 31467,
		"sha512": "bb9488ec5bd24b84f4605a96b53e33dd22ebcd223314ba11833d78fa4587e9c25785390f0b7a81d4716730d9ea8856f782f7285f4824f8a5020e27b26059c837",
		"sha256": "15c8614f493cf081b53ea379b06d759fc51cf94d61d245baab5efb77648bf8d4",
		"sha1": "c0d4386f9b7ee0695a417421839cb43b52eec657",
		"md5": "58c1774e1e76384c1f77820a9b5154c8",
		"ssdeep": "384:q9gkc+hbFKw0zybdKkbxBnalqi6rGsNAYAXJqskGB:q9gk9HxBnwShAZqCB",
		"tlsh": "T16ae246a81045135ad3a322d5ff1af0083b2bfa1959e084e835efc7bd657bdecd612522",
		"mime": "text/rtf",
		"magic": "Rich Text Format data, version 1, ANSI, code page 1252, default middle east language ID 1025",
		"file_format_legacy": "Rich Text Format",
		"file_format": "document/office/rtf",
		"file_extension": "rtf"
	  }`)
	testVerifyMetadata(t, "0dc315e0b3d9f4098ea5cac977b9814e3c6e9116cf296c1bbfcb3ab95c72ca99",
		"Malicious email (MIME HTML Archive), trojan, cve20120158.", `{
		"identify_version": 2,
		"label": "content",
		"size": 268766,
		"sha512": "ebedca9abfd3802114d1049e254c1d5b0f49c03c674fd53c7389d151b5a3f3dedf723cb880d7e569048ee8aa2973018f61b4f960ecc571095c7a198a6a829c27",
		"sha256": "0dc315e0b3d9f4098ea5cac977b9814e3c6e9116cf296c1bbfcb3ab95c72ca99",
		"sha1": "3fbabc390f2f49d379dc0f33f712722906496f2e",
		"md5": "ad55ff065ca5f1a525724b4afad1a540",
		"ssdeep": "3072:kuNbTQ+BDKzdgIBnzDr+H+rL/3SzMMHkTQPP+Rs9QQZv+NZzjAJ9J2PjmPpMMh+:k2nkmi+erTNCDPP+0F2NZzIJ2rmt",
		"tlsh": "T19444dfbbfe441a15c51e963860a3ceb5ae59cdc346a18b0338f977effc715850c46a0a",
		"mime": "application/octet-stream",
		"magic": "data",
		"file_format_legacy": "XML",
		"file_format": "code/xml",
		"file_extension": "xml"
	  }`)
	testVerifyMetadata(t, "03bcc26e07e36ef89486bdb5d25a541916e1e85b5c959868ec069cf9745a9b92",
		"Benign SGML ASCII text.", `{
		"identify_version": 2,
		"label": "content",
		"size": 898,
		"sha512": "9035c01d19f7168ab40d145bb3ce27715b18a6031f8de886dfdbfa760bd17ea627f31eb7425698b66592c723d031ad557ad2e601cf05a04a89588ad2c4c0fe25",
		"sha256": "03bcc26e07e36ef89486bdb5d25a541916e1e85b5c959868ec069cf9745a9b92",
		"sha1": "2b0abf6f5f0b341122923cd8beb03d29716c040e",
		"md5": "da538521f4ce293f4f08d2ecf0215107",
		"tlsh": "T108112f96c92ca1b9b31002c732a0354a19799f7e59cc56fcc0ca8eb9f101ed0d6522c8",
		"mime": "text/plain",
		"magic": "exported SGML document, ASCII text",
		"file_format_legacy": "SGML",
		"file_format": "code/sgml",
		"file_extension": "sgml"
	  }`)
	// Test the very edge of Tlsh
	testVerifyMetadata(t, "c20c75c09736b79f9b5267f9fc5ba712ff648712be3fe3be92a55c56493488ae",
		"Text file that contains the text 'abjklsdjfklasdfklasdlkflksadfjklslkdjfwenuircvuecnn' (51 bytes)", `{
		"identify_version": 2,
		"label": "content",
		"size": 51,
		"sha512": "8d902d799f75b8c0b2fe0eef50bd9efb1c1a124a49ec37cb8cfdeb9dc8962dd98590132a234e4b937bb47074a6403730ab1dc080c62241be07f7eec21c1debe8",
		"sha256": "c20c75c09736b79f9b5267f9fc5ba712ff648712be3fe3be92a55c56493488ae",
		"sha1": "bad6a1f0bce1b6088fdfd7193062477380b80dfb",
		"md5": "84404b3fe4ed7a708206b1c922da5b78",
		"tlsh": "T10090024941001050a42d4313414ddccee457ad15864067e147d96697ca00492739192d",
		"mime": "text/plain",
		"magic": "ASCII text, with no line terminators",
		"file_format_legacy": "Text",
		"file_format": "text/plain",
		"file_extension": "txt"
	}`)
}

func TestMinimal(t *testing.T) {
	testVerifyMetadataMinimal(t, "4d07389cc8be738474d240946a8094be4c89db028958a8e5b28290e4502ec8e2",
		"Benign text file containing the text 'This is some test text data.'", `{
		"label": "",
		"size": 29,
		"sha512": "",
		"sha256": "4d07389cc8be738474d240946a8094be4c89db028958a8e5b28290e4502ec8e2",
		"sha1": "",
		"md5": "",
		"mime": "",
		"magic": ""
	  }`)
	testVerifyMetadataMinimal(t, "25ed3194250f3fc23145ca1d51e58b50004968d31223a89a7736b09377f7be47",
		"Benign text file containing the words 'This is some test text data.' for 795 lines.", ` {
		"label": "",
		"size": 23055,
		"sha512": "",
		"sha256": "25ed3194250f3fc23145ca1d51e58b50004968d31223a89a7736b09377f7be47",
		"sha1": "",
		"md5": "",
		"mime": "",
		"magic": ""
	  }`)
	testVerifyMetadataMinimal(t, "992fa72f92e9b57ede1ba5e7e947f71019f60bde3a33d64d767c9e388f4a766b",
		"Benign password protected ARJ file.", `{
		"label": "",
		"size": 440,
		"sha512": "",
		"sha256": "992fa72f92e9b57ede1ba5e7e947f71019f60bde3a33d64d767c9e388f4a766b",
		"sha1": "",
		"md5": "",
		"mime": "",
		"magic": ""
	  }`)
}
