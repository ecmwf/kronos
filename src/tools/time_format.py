
def format_seconds(s):

    # Round to a whole number of seconds
    s = int(round(s))

    mins, secs = divmod(s, 60)
    out = "{}s".format(secs)

    if mins > 0:
        hrs, mins = divmod(mins, 60)
        out = "{}m {}".format(mins, out)

        if hrs > 0:
            days, hrs = divmod(hrs, 24)
            out = "{}h {}".format(hrs, out)

            if days > 0:
                out = "{}d {}".format(days, out)

    return out


