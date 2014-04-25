package db

import (
    . "github.com/onsi/ginkgo"
    . "github.com/onsi/gomega"
    "testing"
)

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "db")
}


var _ = Describe("DB", func() {
	It("Should be true", func() {
		Expect(true).To(BeTrue())
	})
})

