def tablify(rows):
    if not rows:
        return [u""]
    cols = len(rows[0])
    col_widths = [0] * cols
    for row in rows:
        for pos, col in enumerate(row):
            col_widths[pos] = max(col_widths[pos], len(unicode(col)))
    buf = []
    for row in rows:
        cur_row_buf = []
        for pos, col in enumerate(row):
            cur_row_buf.append(unicode(col).ljust(col_widths[pos] + 2))
        buf.append(u"".join(cur_row_buf))
    return buf
