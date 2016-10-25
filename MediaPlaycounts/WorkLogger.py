import arrow

def success_log(message, save_to="success_log.txt"):
    timestamp = arrow.now().format('YYYY-MM-DD HH:mm:ss')

    with open(save_to, "a") as f:
        f.write(timestamp + "\t" + message + "\n")

def error_log(message, save_to="error_log.txt"):
    timestamp = arrow.now().format('YYYY-MM-DD HH:mm:ss')

    with open(save_to, "a") as f:
        f.write(timestamp + "\t" + message + "\n")
