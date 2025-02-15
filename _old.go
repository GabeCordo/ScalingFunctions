package yule

//// add all the metadata passed to the Pipeline to the local environment

// TODO : possibly enhance security?
//for key, value := range instance.metadata {
//
//if DEBUG {
//log.Printf("added new environment value '%s'\n", key)
//}
//os.Setenv(key, value)
//}

// calculate the timings produced by data being fed across each of the channels
// TODO: support
//instance.CalculateTiming()

//// cleanup environment variables that were dynamically set

//for key, _ := range instance.metadata {
//
//if DEBUG {
//log.Printf("deleted environment value '%s'\n", key)
//}
//os.Unsetenv(key)
//}
