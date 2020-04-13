# Contributing
Contributions are always welcome! Please keep in mind the following guidance:

## Issues/Bug reports
* Please use the search function before opening a new issue
* Please provide all the details necessary to reproduce your issue, including:
  * The version of Go you are using
  * The version of _warp_ you are using
  * The input you are providing
  * The output you are receiving
  * The output you are expecting to receive
* If you know how to fix a reported bug, please submit a pull request!

## Enhancements/Feature requests
* Feature requests are welcome! Please bear in mind the stated design goals and aim for 100% compliance with the WebAuthn Level 1 specification. If your request meets these parameters, submit an issue and it will be reviewed and tagged.

## Pull requests
* Please use the fork and pull request workflow
* Please ensure that backward-compatibility is maintained, or if it isn't, please clearly explain how it was broken. Note that PRs which break backward compatibility may be deferred to a later major release.
* Please ensure that your code has been properly formatted, linted, and vetted using the standard Go tools, and is properly documented. PRs that negatively affect the Go report card will not be merged until corrected.
* Please ensure that your PR has full test coverage and that all tests pass the PR build. No PRs will be merged that reduce test coverage below 100%.