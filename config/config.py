
INTERFACE_IDS = {
    key: "interfaces/mist-ams/control-file.json"
    for key in ("mist-ams", "mist")
} | {
    key: "interfaces/mist-ams/control-file.json"
    for key in ("mist-ams-test", "mist-test")
} | {
    key: "interfaces/dms/control-file.json"
    for key in ("dms", "dms-test")
}
