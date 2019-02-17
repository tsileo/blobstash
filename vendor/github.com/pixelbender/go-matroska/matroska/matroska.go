package matroska

import (
	"github.com/pixelbender/go-matroska/ebml"
	"log"
	"os"
	"time"
)

func Decode(file string) (*File, error) {
	r, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	dec := ebml.NewReader(r, &ebml.DecodeOptions{
		SkipDamaged: true,
	})
	v := new(File)
	if err = dec.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

// File represents a Matroska encoded file.
// See the specification https://matroska.org/technical/specs/index.html
type File struct {
	EBML    *EBML    `ebml:"1A45DFA3"`
	Segment *Segment `ebml:"18538067"`
}

func NewFile(doctype string) *File {
	return &File{
		EBML:    &EBML{1, 1, 4, 8, doctype, 1, 1},
		Segment: &Segment{},
	}
}

// The EBML is a top level element contains a description of the file type.
type EBML struct {
	Version            int    `ebml:"4286,1"`
	ReadVersion        int    `ebml:"42F7,1"`
	MaxIDLength        int    `ebml:"42F2,4"`
	MaxSizeLength      int    `ebml:"42F3,8"`
	DocType            string `ebml:"4282,matroska"`
	DocTypeVersion     int    `ebml:"4287,1"`
	DocTypeReadVersion int    `ebml:"4285,1"`
}

// ID is a binary EBML element identifier.
type ID uint32

// SegmentID is a randomly generated unique 128bit identifier of Segment/SegmentFamily.
type SegmentID []byte

// EditionID is a unique identifier of Edition.
type EditionID []byte

type Position int64
type Time int64
type Duration int64

// Segment is the Root Element that contains all other Top-Level Elements.
type Segment struct {
	SeekHead    []*SeekHead   `ebml:"114D9B74,omitempty" json:",omitempty"`
	Info        []*Info       `ebml:"1549A966" json:",omitempty"`
	Cluster     []*Cluster    `ebml:"1F43B675,omitempty" json:",omitempty"`
	Tracks      []*Track      `ebml:"1654AE6B,omitempty" json:",omitempty"`
	Cues        []*CuePoint   `ebml:"1C53BB6B>BB,omitempty" json:",omitempty"`
	Attachments []*Attachment `ebml:"1941A469>61A7"`
	Chapters    []*Edition    `ebml:"1043A770>45B9"`
	Tags        []*Tag        `ebml:"1254C367>7373"`
}

// SeekHead contains the position of other Top-Level Elements.
type SeekHead struct {
	Seeks []*Seek `ebml:"4DBB"`
}

// Seek contains a single seek entry to an EBML Element.
type Seek struct {
	ID       ID       `ebml:"53AB"`
	Position Position `ebml:"53AC"`
}

// Info contains miscellaneous general information and statistics on the file.
type Info struct {
	ID               SegmentID           `ebml:"73A4,omitempty" json:",omitempty"`
	Filename         string              `ebml:"7384,omitempty" json:",omitempty"`
	PrevID           SegmentID           `ebml:"3CB923,omitempty" json:",omitempty"`
	PrevFilename     string              `ebml:"3C83AB,omitempty" json:",omitempty"`
	NextID           SegmentID           `ebml:"3EB923,omitempty" json:",omitempty"`
	NextFilename     string              `ebml:"3E83BB,omitempty" json:",omitempty"`
	SegmentFamily    SegmentID           `ebml:"4444,omitempty" json:",omitempty"`
	ChapterTranslate []*ChapterTranslate `ebml:"6924,omitempty" json:",omitempty"`
	TimecodeScale    time.Duration       `ebml:"2AD7B1,1000000"`
	Duration         float64             `ebml:"4489,omitempty" json:",omitempty"`
	Date             time.Time           `ebml:"4461,omitempty" json:",omitempty"`
	Title            string              `ebml:"7BA9,omitempty" json:",omitempty"`
	MuxingApp        string              `ebml:"4D80"`
	WritingApp       string              `ebml:"5741"`
}

// ChapterTranslate contains tuple of corresponding ID used by chapter codecs to represent a Segment.
type ChapterTranslate struct {
	EditionIDs []EditionID  `ebml:"69FC,omitempty" json:",omitempty"`
	Codec      ChapterCodec `ebml:"69BF"`
	ID         TranslateID  `ebml:"69A5"`
}

type TranslateID []byte
type ChapterCodec uint8

const (
	ChapterCodecMatroska ChapterCodec = iota
	ChapterCodecDVD
)

// Cluster is a Top-Level Element containing the Block structure.
type Cluster struct {
	Timecode     Time          `ebml:"E7"`
	SilentTracks []TrackNumber `ebml:"5854>58D7,omitempty" json:",omitempty"`
	Position     Position      `ebml:"A7,omitempty" json:",omitempty"`
	PrevSize     int64         `ebml:"AB,omitempty" json:",omitempty"`
	SimpleBlock  []*Block      `ebml:"A3,omitempty" json:",omitempty"`
	BlockGroup   []*BlockGroup `ebml:"A0,omitempty" json:",omitempty"`
}

type ClusterID uint64

// Block contains the actual data to be rendered and a timestamp.
type Block struct {
	TrackNumber TrackNumber
	Timecode    int16
	Flags       uint8
	Frames      int
	//Data []byte
}

const (
	LacingNone uint8 = iota
	LacingXiph
	LacingFixedSize
	LacingEBML
)

func (b *Block) UnmarshalEBML(r *ebml.Reader) error {
	//log.Printf("\t Block %#v", r.Len())
	v, err := r.ReadVInt()
	if err != nil {
		log.Println(err)
		return err
	}
	b.TrackNumber = TrackNumber(v)
	p, err := r.Next(3)
	if err != nil {
		log.Println(err)
		return err
	}
	b.Timecode = int16(p[0])<<8 | int16(p[1])
	b.Flags = p[2]
	if (b.Flags>>1)&3 == 0 {
		return nil
	}
	log.Printf("\t Block %#v", b)
	p, err = r.Next(1)
	if err != nil {
		log.Println(err)
		return err
	}
	b.Frames = int(p[0])
	log.Printf("\t Block %#v", b)
	return nil
}

// BlockGroup contains a single Block and a relative information.
type BlockGroup struct {
	Block             *Block           `ebml:"A1" json:",omitempty"`
	Additions         []*BlockAddition `ebml:"75A1>A6,omitempty" json:",omitempty"`
	Duration          Duration         `ebml:"9B,omitempty" json:",omitempty"`
	ReferencePriority int64            `ebml:"FA"`
	ReferenceBlock    []Time           `ebml:"FB,omitempty" json:",omitempty"`
	CodecState        []byte           `ebml:"A4,omitempty" json:",omitempty"`
	DiscardPadding    time.Duration    `ebml:"75A2,omitempty" json:",omitempty"`
	Slices            []*TimeSlice     `ebml:"8E>E8,omitempty" json:",omitempty"`
}

type TimeSlice struct {
	LaceNumber int64 `ebml:"CC"`
}

// BlockAdd contains additional blocks to complete the main one.
type BlockAddition struct {
	ID   BlockAdditionID `ebml:"EE,1"`
	Data []byte          `ebml:"A5"`
}

type BlockAdditionID uint64

// Track is a Top-Level Element of information with track description.
type Track struct {
	Entries []*TrackEntry `ebml:"AE"`
}

// TrackEntry describes a track with all Elements.
type TrackEntry struct {
	Number                      TrackNumber        `ebml:"D7"`
	ID                          TrackID            `ebml:"73C5"`
	Type                        TrackType          `ebml:"83"`
	Enabled                     bool               `ebml:"B9,true"`
	Default                     bool               `ebml:"88,true"`
	Forced                      bool               `ebml:"55AA"`
	Lacing                      bool               `ebml:"9C,true"`
	MinCache                    int                `ebml:"6DE7"`
	MaxCache                    int                `ebml:"6DF8,omitempty" json:",omitempty"`
	DefaultDuration             time.Duration      `ebml:"23E383,omitempty" json:",omitempty"`
	DefaultDecodedFieldDuration time.Duration      `ebml:"234E7A,omitempty" json:",omitempty"`
	MaxBlockAdditionID          BlockAdditionID    `ebml:"55EE"`
	Name                        string             `ebml:"536E,omitempty" json:",omitempty"`
	Language                    string             `ebml:"22B59C,eng,omitempty" json:",omitempty"`
	CodecID                     string             `ebml:"86"`
	CodecPrivate                []byte             `ebml:"63A2,omitempty" json:",omitempty"`
	CodecName                   string             `ebml:"258688,omitempty" json:",omitempty"`
	AttachmentLink              AttachmentID       `ebml:"7446,omitempty" json:",omitempty"`
	CodecDecodeAll              bool               `ebml:"AA,true"`
	TrackOverlay                []TrackNumber      `ebml:"6FAB,omitempty" json:",omitempty"`
	CodecDelay                  time.Duration      `ebml:"56AA,omitempty" json:",omitempty"`
	SeekPreRoll                 time.Duration      `ebml:"56BB"`
	TrackTranslate              []*TrackTranslate  `ebml:"6624,omitempty" json:",omitempty"`
	Video                       *VideoTrack        `ebml:"E0,omitempty" json:",omitempty"`
	Audio                       *AudioTrack        `ebml:"E1,omitempty" json:",omitempty"`
	TrackOperation              *TrackOperation    `ebml:"E2,omitempty" json:",omitempty"`
	ContentEncodings            []*ContentEncoding `ebml:"6D80>6240,omitempty" json:",omitempty"`
}

type TrackID uint64
type TrackNumber int
type AttachmentID uint8
type TrackType uint8

const (
	TrackTypeVideo    TrackType = 0x01
	TrackTypeAudio    TrackType = 0x02
	TrackTypeComplex  TrackType = 0x03
	TrackTypeLogo     TrackType = 0x10
	TrackTypeSubtitle TrackType = 0x11
	TrackTypeButton   TrackType = 0x12
	TrackTypeControl  TrackType = 0x20
)

// TrackTranslate describes a track identification for the given Chapter Codec.
type TrackTranslate struct {
	EditionIDs       []EditionID  `ebml:"66FC,omitempty" json:",omitempty"`
	Codec            ChapterCodec `ebml:"66BF"`
	TranslateTrackID `ebml:"66A5"`
}

type TranslateTrackID []byte

// VideoTrack contains information that is specific for video tracks.
type VideoTrack struct {
	Interlaced      InterlaceType   `ebml:"9A"`
	FieldOrder      FieldOrder      `ebml:"9D,2"`
	StereoMode      StereoMode      `ebml:"53B8,omitempty" json:"stereoMode,omitempty"`
	AlphaMode       *AlphaMode      `ebml:"53C0,omitempty" json:"alphaMode,omitempty"`
	Width           int             `ebml:"B0"`
	Height          int             `ebml:"BA"`
	CropBottom      int             `ebml:"54AA,omitempty" json:",omitempty"`
	CropTop         int             `ebml:"54BB,omitempty" json:",omitempty"`
	CropLeft        int             `ebml:"54CC,omitempty" json:",omitempty"`
	CropRight       int             `ebml:"54DD,omitempty" json:",omitempty"`
	DisplayWidth    int             `ebml:"54B0,omitempty" json:",omitempty"`
	DisplayHeight   int             `ebml:"54BA,omitempty" json:",omitempty"`
	DisplayUnit     DisplayUnit     `ebml:"54B2,omitempty" json:",omitempty"`
	AspectRatioType AspectRatioType `ebml:"54B3,omitempty" json:",omitempty"`
	ColourSpace     uint32          `ebml:"2EB524,omitempty" json:",omitempty"`
	Colour          *Colour         `ebml:"55B0,omitempty" json:",omitempty"`
}

type InterlaceType uint8

// InterlaceTypes
const (
	InterlaceTypeInterlaced  InterlaceType = 1
	InterlaceTypeProgressive InterlaceType = 2
)

type FieldOrder uint8

// FieldOrders
const (
	FieldOrderProgressive           FieldOrder = 0
	FieldOrderTop                   FieldOrder = 1
	FieldOrderUndetermined          FieldOrder = 2
	FieldOrderBottom                FieldOrder = 6
	FieldOrderDisplayBottomStoreTop FieldOrder = 9
	FieldOrderDisplayTopStoreBottom FieldOrder = 14
)

type StereoMode uint8

// StereoModes
const (
	StereoModeMono StereoMode = iota
	StereoModeHorizontalLeft
	StereoModeVerticalRight
	StereoModeVerticalLeft
	StereoModeCheckboardRight
	StereoModeCheckboardLeft
	StereoModeInterleavedRight
	StereoModeInterleavedLeft
	StereoModeColumnInterleavedRight
	StereoModeAnaglyphCyanRed
	StereoModeHorizontalRight
	StereoModeAnaglyphGreenMagenta
	StereoModeLacedLeft
	StereoModeLacedRight
)

type AlphaMode struct {
}

type DisplayUnit uint8

// DisplayUnits
const (
	DisplayUnitPixels DisplayUnit = iota
	DisplayUnitCentimeters
	DisplayUnitInches
	DisplayUnitAspectRatio
)

type AspectRatioType uint8

// AspectRatioTypes
const (
	AspectRatioFreeResizing AspectRatioType = iota
	AspectRatioKeep
	AspectRatioFixed
)

// Colour describes the colour format settings.
type Colour struct {
	MatrixCoefficients      MatrixCoefficients      `ebml:"55B1,2,omitempty" json:",omitempty"`
	BitsPerChannel          int                     `ebml:"55B2,omitempty" json:",omitempty"`
	ChromaSubsamplingHorz   int                     `ebml:"55B3,omitempty" json:",omitempty"`
	ChromaSubsamplingVert   int                     `ebml:"55B4,omitempty" json:",omitempty"`
	CbSubsamplingHorz       int                     `ebml:"55B5,omitempty" json:",omitempty"`
	CbSubsamplingVert       int                     `ebml:"55B6,omitempty" json:",omitempty"`
	ChromaSitingHorz        ChromaSiting            `ebml:"55B7,omitempty" json:",omitempty"`
	ChromaSitingVert        ChromaSiting            `ebml:"55B8,omitempty" json:",omitempty"`
	ColourRange             ColourRange             `ebml:"55B9,omitempty" json:",omitempty"`
	TransferCharacteristics TransferCharacteristics `ebml:"55BA,omitempty" json:",omitempty"`
	Primaries               Primaries               `ebml:"55BB,2,omitempty" json:",omitempty"`
	MaxCLL                  int64                   `ebml:"55BC,omitempty" json:",omitempty"`
	MaxFALL                 int64                   `ebml:"55BD,omitempty" json:",omitempty"`
	MasteringMetadata       *MasteringMetadata      `ebml:"55D0"`
}

// MatrixCoefficients, see Table 4 of ISO/IEC 23001-8:2013/DCOR1
type MatrixCoefficients uint8

// TransferCharacteristics, see Table 3 of ISO/IEC 23001-8:2013/DCOR1
type TransferCharacteristics uint8

// Primaries, see Table 2 of ISO/IEC 23001-8:2013/DCOR1
type Primaries uint8

type ChromaSiting uint8

// ChromaSitings
const (
	ChromaSitingUnspecified ChromaSiting = iota
	ChromaSitingCollocated
	ChromaSitingHalf
)

type ColourRange uint8

// ColourRange
const (
	ColourRangeUnspecified ColourRange = iota
	ColourRangeBroadcast
	ColourRangeFull
	ColourRangeDefined
)

// MasteringMetadata represents SMPTE 2086 mastering data.
type MasteringMetadata struct {
	PrimaryRChromaX   float64 `ebml:"55D1,omitempty" json:",omitempty"`
	PrimaryRChromaY   float64 `ebml:"55D2,omitempty" json:",omitempty"`
	PrimaryGChromaX   float64 `ebml:"55D3,omitempty" json:",omitempty"`
	PrimaryGChromaY   float64 `ebml:"55D4,omitempty" json:",omitempty"`
	PrimaryBChromaX   float64 `ebml:"55D5,omitempty" json:",omitempty"`
	PrimaryBChromaY   float64 `ebml:"55D6,omitempty" json:",omitempty"`
	WhitePointChromaX float64 `ebml:"55D7,omitempty" json:",omitempty"`
	WhitePointChromaY float64 `ebml:"55D8,omitempty" json:",omitempty"`
	LuminanceMax      float64 `ebml:"55D9,omitempty" json:",omitempty"`
	LuminanceMin      float64 `ebml:"55DA,omitempty" json:",omitempty"`
}

// AudioTrack contains information that is specific for audio tracks.
type AudioTrack struct {
	SamplingFreq       float64 `ebml:"B5,8000"`
	OutputSamplingFreq float64 `ebml:"78B5,omitempty" json:",omitempty"`
	Channels           int     `ebml:"9F,1"`
	BitDepth           int     `ebml:"6264,omitempty" json:",omitempty"`
}

// TrackOperation describes an operation that needs to be applied on tracks
// to create the virtual track.
type TrackOperation struct {
	CombinePlanes []*TrackPlane `ebml:"E3>E4,omitempty" json:",omitempty"`
	JoinBlocks    []TrackID     `ebml:"E9>ED,omitempty" json:",omitempty"`
}

// TrackPlane contains a video plane track that need to be combined to create this track.
type TrackPlane struct {
	ID   TrackID   `ebml:"E5"`
	Type PlaneType `ebml:"E6"`
}

type PlaneType uint8

// PlaneTypes
const (
	PlaneTypeLeft PlaneType = iota
	PlaneTypeRight
	PlaneTypeBackground
)

// ContentEncoding contains settings for several content encoding mechanisms
// like compression or encryption.
type ContentEncoding struct {
	Order       int           `ebml:"5031"`
	Scope       EncodingScope `ebml:"5032,1"`
	Type        EncodingType  `ebml:"5033"`
	Compression *Compression  `ebml:"5034,omitempty" json:",omitempty"`
	Encryption  *Encryption   `ebml:"5035,omitempty" json:",omitempty"`
}

type EncodingScope uint8

// EncodingScopes
const (
	EncodingScopeAll     EncodingScope = 1
	EncodingScopePrivate EncodingScope = 2
	EncodingScopeNext    EncodingScope = 4
)

type EncodingType uint8

const (
	EncodingTypeCompression EncodingType = iota
	EncodingTypeEncryption
)

// Compression describes the compression used.
type Compression struct {
	Algo     CompressionAlgo `ebml:"4254"`
	Settings []byte          `ebml:"4255,omitempty" json:",omitempty"`
}

type CompressionAlgo uint8

const (
	CompressionAlgoZlib            CompressionAlgo = 0
	CompressionAlgoHeaderStripping CompressionAlgo = 3
)

// Encryption describes the encryption used.
type Encryption struct {
	Algo         uint8  `ebml:"47E1,omitempty" json:",omitempty"`
	KeyID        []byte `ebml:"47E2,omitempty" json:",omitempty"`
	Signature    []byte `ebml:"47E3,omitempty" json:",omitempty"`
	SignKeyID    []byte `ebml:"47E4,omitempty" json:",omitempty"`
	SignAlgo     uint8  `ebml:"47E5,omitempty" json:",omitempty"`
	SignHashAlgo uint8  `ebml:"47E6,omitempty" json:",omitempty"`
}

// CuePoint contains all information relative to a seek point in the Segment.
type CuePoint struct {
	Time           Time                `ebml:"B3"`
	TrackPositions []*CueTrackPosition `ebml:"B7"`
}

// CueTrackPosition contains positions for different tracks corresponding to the timestamp.
type CueTrackPosition struct {
	Track            TrackNumber `ebml:"F7"`
	ClusterPosition  Position    `ebml:"F1"`
	RelativePosition Position    `ebml:"F0,omitempty" json:",omitempty"`
	Duration         Duration    `ebml:"B2,omitempty" json:",omitempty"`
	BlockNumber      int         `ebml:"5378,1,omitempty" json:",omitempty"`
	CodecState       Position    `ebml:"EA,omitempty" json:",omitempty"`
	References       []Time      `ebml:"DB>96,omitempty" json:",omitempty"`
}

// Attachment describes attached files.
type Attachment struct {
	ID          AttachmentID `ebml:"46AE"`
	Description string       `ebml:"467E,omitempty" json:",omitempty"`
	Name        string       `ebml:"466E"`
	MimeType    string       `ebml:"4660"`
	Data        []byte       `ebml:"465C"`
}

// Edition contains all information about a Segment edition.
type Edition struct {
	ID      EditionID      `ebml:"45BC,omitempty" json:",omitempty"`
	Hidden  bool           `ebml:"45BD"`
	Default bool           `ebml:"45DB"`
	Ordered bool           `ebml:"45DD,omitempty" json:",omitempty"`
	Atoms   []*ChapterAtom `ebml:"B6"`
}

// ChapterAtom contains the atom information to use as the chapter atom.
type ChapterAtom struct {
	ID            ChapterID         `ebml:"73C4"`
	StringID      string            `ebml:"5654,omitempty" json:",omitempty"`
	TimeStart     Time              `ebml:"91"`
	TimeEnd       Time              `ebml:"92,omitempty" json:",omitempty"`
	Hidden        bool              `ebml:"98"`
	Enabled       bool              `ebml:"4598,true"`
	SegmentID     SegmentID         `ebml:"6E67,omitempty" json:",omitempty"`
	EditionID     EditionID         `ebml:"6EBC,omitempty" json:",omitempty"`
	PhysicalEquiv int               `ebml:"63C3,omitempty" json:",omitempty"`
	Tracks        []TrackID         `ebml:"8F>89,omitempty" json:",omitempty"`
	Displays      []*ChapterDisplay `ebml:"80,omitempty" json:",omitempty"`
	Processes     []*ChapterProcess `ebml:"6944,omitempty" json:",omitempty"`
}

type ChapterID uint64

// ChapterDisplay contains all possible strings to use for the chapter display.
type ChapterDisplay struct {
	String   string `ebml:"85"`
	Language string `ebml:"437C,eng"`                         // See ISO-639-2
	Country  string `ebml:"437E,omitempty" json:",omitempty"` // See IANA ccTLDs
}

// ChapterProcess describes the atom processing commands.
type ChapterProcess struct {
	CodecID ChapterCodec      `ebml:"6955"`
	Private []byte            `ebml:"450D,omitempty" json:",omitempty"`
	Command []*ChapterCommand `ebml:"6911,omitempty" json:",omitempty"`
}

// ChapterCommand contains all the commands associated to the atom.
type ChapterCommand struct {
	Time Time   `ebml:"6922"`
	Data []byte `ebml:"6933"`
}

// Tag contains Elements specific to Tracks/Chapters.
type Tag struct {
	Targets    []*Target    `ebml:"63C0"`
	SimpleTags []*SimpleTag `ebml:"67C8"`
}

// Target contains all IDs where the specified meta data apply.
type Target struct {
	TypeValue     int            `ebml:"68CA,50,omitempty" json:",omitempty"`
	Type          string         `ebml:"63CA,omitempty" json:",omitempty"`
	TrackIDs      []TrackID      `ebml:"63C5,omitempty" json:",omitempty"`
	EditionIDs    []EditionID    `ebml:"63C9,omitempty" json:",omitempty"`
	ChapterIDs    []ChapterID    `ebml:"63C4,omitempty" json:",omitempty"`
	AttachmentIDs []AttachmentID `ebml:"63C6,omitempty" json:",omitempty"`
}

// SimpleTag contains general information about the target.
type SimpleTag struct {
	Name     string `ebml:"45A3"`
	Language string `ebml:"447A,und"`
	Default  bool   `ebml:"4484,true"`
	String   string `ebml:"4487,omitempty" json:",omitempty"`
	Binary   []byte `ebml:"4485,omitempty" json:",omitempty"`
}

func NewSimpleTag(name, text string) *SimpleTag {
	return &SimpleTag{
		Name:     name,
		String:   text,
		Language: "und",
		Default:  true,
	}
}
