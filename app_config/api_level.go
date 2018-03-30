package app_config


/*
	open the file -- get its contents -- close it -- run checks on it -- if checks come out good --
	run a populate on it and add it to the main struct
	put the contents in the struct

 */

type TablesRef []Table

type Table struct {

}

var TablesConf TablesRef

/*
Does these checks:
	- Has this guy put in duplicate values in this array, if yes, then it's not good cause you'll have multiple objects
	- Do we have references for the fields to reach the toml files in the main config file?
		- If not, exit -- we wont be able to do anything with the applicatoin without this configuration
	- Does the directory mentioned for the reference exist?
	- Do all the files that are mentioned in the config file exist?

	- Open each file take contents of the file -- send it to a checker -- if all looks good then put the
		contents into the struct

 */
func BasicChecks() (err error){

	// Make sure we don't have duplicates in the array .. if yes throw an error

	// Make sure the folder mentioned for the reference exists

	// Make sure each file mentioned in the reference exists

	// For each file:
		// Open the file, get the contents, close the file

		// Pass these references into a checker to validate if its okay -- return back a base toml return from here

		// If the validator comes back without errors, send the base toml references to a populate function to fill
		// the module struct with the details of the configuration

	return
}

/*
	Given the toml contents -- run checks on and return errors if any

 */
func validateToml(confs map[string]string)(err error) {

	return
}


/*
	Given the toml contents parse and populate the final struct that will be used by the application
		return any errors if found
 */
func populateToml(confs map[string]string)(err error) {


	return
}











