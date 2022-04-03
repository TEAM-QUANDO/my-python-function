def format_bytes(bytes: int, unit: str, SI: bool = False):
    """
    Converts bytes to common units such as kb, kib, KB, mb, mib, MB

    Parameters
    ---------
    bytes: int
        Number of bytes to be converted

    unit: str
        Desired unit of measure for output


    SI: bool
        True -> Use SI standard e.g. KB = 1000 bytes
        False -> Use JEDEC standard e.g. KB = 1024 bytes

    Returns
    -------
    str:
        E.g. "7 MiB" where MiB is the original unit abbreviation supplied
    """
    if unit.lower() in "b bit bits".split():
        return f"{bytes * 8} {unit}"
    unitN = unit[0].upper() + unit[1:].replace("s", "")  # Normalised
    reference = {"Kb Kib Kibibit Kilobit": (7, 1),
                 "KB KiB Kibibyte Kilobyte": (10, 1),
                 "Mb Mib Mebibit Megabit": (17, 2),
                 "MB MiB Mebibyte Megabyte": (20, 2),
                 "Gb Gib Gibibit Gigabit": (27, 3),
                 "GB GiB Gibibyte Gigabyte": (30, 3),
                 "Tb Tib Tebibit Terabit": (37, 4),
                 "TB TiB Tebibyte Terabyte": (40, 4),
                 "Pb Pib Pebibit Petabit": (47, 5),
                 "PB PiB Pebibyte Petabyte": (50, 5),
                 "Eb Eib Exbibit Exabit": (57, 6),
                 "EB EiB Exbibyte Exabyte": (60, 6),
                 "Zb Zib Zebibit Zettabit": (67, 7),
                 "ZB ZiB Zebibyte Zettabyte": (70, 7),
                 "Yb Yib Yobibit Yottabit": (77, 8),
                 "YB YiB Yobibyte Yottabyte": (80, 8),
                 }
    key_list = '\n'.join(["     b Bit"] + [x for x in reference.keys()]) + "\n"
    if unitN not in key_list:
        raise IndexError(f"\n\nConversion unit must be one of:\n\n{key_list}")
    units, divisors = [(k, v) for k, v in reference.items() if unitN in k][0]
    if SI:
        divisor = 1000**divisors[1]/8 if "bit" in units else 1000**divisors[1]
    else:
        divisor = float(1 << divisors[0])
    value = bytes / divisor
    return f"{value:,.0f} {unitN}{(value != 1 and len(unitN) > 3)*'s'}"


assert format_bytes(1, "b") == '8 b'
assert format_bytes(1, "bits") == '8 bits'
assert format_bytes(1024, "kilobyte") == "1 Kilobyte"
assert format_bytes(1024, "kB") == "1 KB"
assert format_bytes(7141000, "mb") == '54 Mb'
assert format_bytes(7141000, "mib") == '54 Mib'
assert format_bytes(7141000, "Mb") == '54 Mb'
assert format_bytes(7141000, "MB") == '7 MB'
assert format_bytes(7141000, "mebibytes") == '7 Mebibytes'
assert format_bytes(7141000, "gb") == '0 Gb'
assert format_bytes(1000000, "kB") == '977 KB'
assert format_bytes(1000000, "kB", SI=True) == '1,000 KB'
assert format_bytes(1000000, "kb") == '7,812 Kb'
assert format_bytes(1000000, "kb", SI=True) == '8,000 Kb'
assert format_bytes(125000, "kb") == '977 Kb'
assert format_bytes(125000, "kb", SI=True) == '1,000 Kb'
assert format_bytes(125*1024, "kb") == '1,000 Kb'
assert format_bytes(125*1024, "kb", SI=True) == '1,024 Kb'
