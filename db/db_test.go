package db

import (
    . "github.com/onsi/ginkgo"
    . "github.com/onsi/gomega"
    "testing"
    "os"
)

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "db")
}

var _ = Describe("DB", func() {
	db := New("test_db")

	It("Should create the db", func() {
		_, err := os.Stat(db.ldb_path)
		Expect(err).NotTo(HaveOccurred())
	})

	It("low level operations: put/get/getset/del/incrby", func() {
		err := db.put([]byte("foo"), []byte("bar"))
		Expect(err).NotTo(HaveOccurred())
		
		val, err := db.get([]byte("foo"))
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal([]byte("bar")))

		val, err = db.get([]byte("foo2"))
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeNil())

		val, err = db.getset([]byte("foo"), []byte("bar2"))
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal([]byte("bar")))

		val, err = db.get([]byte("foo"))
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal([]byte("bar2")))

		err = db.del([]byte("foo"))
		Expect(err).NotTo(HaveOccurred())
	
		val, err = db.get([]byte("foo"))
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeNil())

		err = db.put([]byte("fooo"), []byte("10"))
		Expect(err).NotTo(HaveOccurred())

		err = db.incrby([]byte("fooo"), 50)
		Expect(err).NotTo(HaveOccurred())

		val, err = db.get([]byte("fooo"))
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal([]byte("60")))
			
		
	})
	It("low level binary stored uint32 value", func() {
		val, err := db.getUint32([]byte("foo2"))
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal(uint32(0)))

		err = db.putUint32([]byte("foo2"), uint32(5))
		Expect(err).NotTo(HaveOccurred())

		val, err = db.getUint32([]byte("foo2"))
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal(uint32(5)))

		err = db.incrUint32([]byte("foo2"), 2)
		Expect(err).NotTo(HaveOccurred())

		val, err = db.getUint32([]byte("foo2"))
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal(uint32(7)))
	})

	It("Should remove the db", func() {
		db.Close()
		err := db.Destroy()
		Expect(err).NotTo(HaveOccurred())
		_, err = os.Stat(db.ldb_path)
		Expect(os.IsNotExist(err)).To(BeTrue())
	})
})
