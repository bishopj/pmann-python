import json
import codecs
import ijson
import csv
from collections import defaultdict
import re
from ast import literal_eval
import ast
from pathlib import Path
from datetime import datetime, date
from typing import Any


"""
High speed/low memory csv & json reading/writing conversion routines for very large files.
Library Contains:
    Top level routines:
    def csv_reader(file, partial_predicate_fn=None, has_header=True, defined_column_names=None, use_column_types=True, column_types=None, encoding="utf-8-sig"):
    def json_reader(file, partial_predicate_fn=None, encoding="utf-8", json_type="auto"):
    def csv_to_json(input_csv, output_json, sep=".", unflatten=True, parse_embedded_json=False, **reader_kwargs):
    def json_to_csv(input_json, output_csv, fieldnames=None, flatten=True, flatten_lists=False, sep='.', encoding="utf-8", **reader_kwargs):
    def load_dict_from_json(file: str, use_type_hints: bool = True) -> dict:
    def save_dict_to_json(data: dict, file: str, save_type_hints: bool = True):
    def infer_column_types(header, sample_rows) -> dict:
    def detect_encoding_from_bom(raw: bytes) -> str:
    def detect_encoding_from_jason_bom(raw: bytes) -> tuple[str, int]:
    def check_json_quotes(file, encoding="utf-8") -> bool:
    def recursive_defaultdict():
    
Helper routines:
    def try_flatten_embedded_json(row, sep="."):
    def scan_json_quotes(file, encoding="utf-8"):
    def dictify(d):
    def unflatten_dict(d, sep='.'):
    def parse_maybe_json(val):
    def csv_to_json2(input_csv, output_json, **reader_kwargs):
    def flatten_dict(d, flatten=True, flatten_lists=False, parent_key='', sep='.', override_keys=None, _position=None):
    def flatten_keys(d, flatten=True, flatten_lists=False, parent_key='', sep='.'):
    
Authors:
        csv_reader - Jonathan Bishop & Daniel Buckley
        all others - Jonathan Bishop
"""

def detect_encoding_from_bom(raw: bytes) -> str:
    if raw.startswith(codecs.BOM_UTF8):
        return "utf-8-sig"
    elif raw.startswith(codecs.BOM_UTF32_LE):
        return "utf-32"
    elif raw.startswith(codecs.BOM_UTF32_BE):
        return "utf-32"
    elif raw.startswith(codecs.BOM_UTF16_LE):
        return "utf-16"
    elif raw.startswith(codecs.BOM_UTF16_BE):
        return "utf-16"
    else:
        return "utf-8-sig"  # <- Safer fallback

def infer_column_types(header, sample_rows) -> dict:
    """infer_column_type 
    Infers the types of the columns in a list of lists 
    containing strings representing data in columns and
    returns a type map  as a dictionary indexed by the 
    column header names with data types as the values

    Args:
        header (list): names of the columns
        sample_rows (list of lists of strings): several rows of data in columns
    Returns:
        dict: Type map of column header name to data type
    """
    type_map = {}
    for i, column in enumerate(header):
        values = [row[i] for row in sample_rows if len(row) > i]
        types = set()
        for val in values:
            try:
                int(val)
                types.add(int)
            except:
                try:
                    float(val)
                    types.add(float)
                except:
                    types.add(str)
        # Choose the most specific type
        if int in types and float not in types:
            type_map[column] = int
        elif float in types:
            type_map[column] = float
        else:
            type_map[column] = str
    return type_map


def try_flatten_embedded_json(row, sep="."):

    new_row = {}

    for k, v in row.items():
        if isinstance(v, str) and v.strip().startswith(("{","[")) and v.strip().endswith(("}","]")):
            try:
                parsed = json.loads(v)
            except json.JSONDecodeError:
                try:
                    parsed = literal_eval(v)
                except Exception:
                    parsed = None

            if isinstance(parsed, dict):
                flat = flatten_dict(parsed, flatten_lists=True, parent_key=k, sep=sep)
                new_row.update(flat)
                continue

        new_row[k] = v  # preserve original
    return new_row


def csv_reader(file, partial_predicate_fn=None, has_header=True, defined_column_names=None, use_column_types=True, column_types=None, encoding="utf-8-sig"):
    """
    Generator based read of a csv yielding one row at a time in a column dictionary.
    If defaults chosen, a header is expected in the first row of the csv and every row after will be yielded,
    utf-8-sig encoding will be assumed and skipped if present in file, column names & types will be inferred from the file
    and column types will be used.

    Optional settings:
    The default encoding is "UTF-8-sig".  If an alternative encoding is supplied that encoding will be assumed when
    opening the file for reading, but if "check" is supplied as the encoding value, the file will first be checked
    for an embedded BOM encoding and that encoding used.  If no encoding is found "utf-8-sig" will be assumed, which
    will also handle the scenario where no encoding is embedded in the file.
    Valid encodings are: "utf-8-sig", "utf-32", "utf-16".  "utf-8" is allowed but discouraged in favour of "utf-8-sig"
    which will handle "utf-8" and BE/LE variations of other encodings are handled automatically and do not need to be
    separately specified.

    If defined_column_names is supplied as a [list] of column names that will take precedence over the
    names stored in the file, but the defined list will be checked for length to match the number of columns in the file. If
    has_header is set to False, and defined_column_names = None but column_types is supplied, column names will be generated
    as a list of the keys from the column_types type map. If has_header is set to False, defined_column_names = None and
    column_types is None a list of column indexes 0 through n will be generated as column names and used in the filter
    predicate, column_types and yielded row dictionary as required.

    If column_types is supplied and use_column_types=True that will be used in preference to inferred column types.
    If column_types=None and use_column_types=True the first 10 rows of the file will be read and the column types
    inferred from the values in the columns. In either case value will be returned as the appropriate type for that column,
    as dictated by the column_types. If use_column_types=False, all values will be returned as strings in their respective
    fields.

    If a partial_predicate is provided it should be a function expecting 2 parameters: (row_index, row) where row_index is an int
    representing the row number starting from 0 AFTER any header row, and row is a row dictionary of {column_name : value}.
    The predicate function should return True if the row is to be included and False if it is to be ignored.

    Authors: Jonathan Bishop & Daniel Buckley

    Args:
        file (str): file path to read
        partial_predicate_fn (function, optional): filter function taking (row_index=row_index, row=row_dict) as arguments . Defaults to None.
        has_header (bool, optional): Whether the csv has a header row. Defaults to True.
        defined_column_names (_type_, optional): optional list of column names in order. Defaults to None.
        use_column_types (bool, optional): Whether to use column types. Defaults to True.
        column_types (_type_, optional): Otional type map dictionary (ColName: type). Defaults to None.
        encoding (str, optional):Optional text encoding or Check. Defaults to "utf-8-sig".

    Raises:
        ValueError: Cannot associate CSV columns with a name
        ValueError: Mismatch in actual ({actual}) and expected ({expected}) column count
        ValueError: Supplied column types column count wrong.  Did you supply a correct type map?
        ValueError: Mismatch in actual ({actual}) and expected ({expected}) types column count

    Yields:
        dict: dictionary of (column_name : value) for each row
    """

    if not has_header and defined_column_names is None:
        raise ValueError("Cannot associate CSV columns with a name")


    if encoding.lower()=="check":
        with open(file, "rb") as f:
            raw = f.read(4)
            encoding = detect_encoding_from_bom(raw)  # Will default to "utf-8-sig"

    with open(file, mode="r", encoding=encoding) as f:
        reader = csv.reader(f)
        header = next(reader)

        if use_column_types and not defined_column_names and column_types:
            defined_column_names = column_types.keys()

        if defined_column_names:
            if has_header and header:
                #Column names supplied
                actual, expected = len(header), len(defined_column_names)
            else:
                #Column names supplied, but no header in file so check column count
                actual, expected = len(header), len(defined_column_names)
                #No header in file, so rewind the fp and recreate the generator
                f.seek(0)
                reader = csv.reader(f)
            if actual != expected:
                raise ValueError(f"Mismatch in actual ({actual}) and expected ({expected}) column count")
            #No error so set the header to the supplied column names
            header = defined_column_names
        else: #No supplied column names so use the ones read from the file, unless no header
            if not has_header:
                #Generate list of column names as numbers 0 to n
                header = [f"{i}" for i in range(len(header))]
                #No header in file, so rewind the fp and recreate the generator
                f.seek(0)
                reader = csv.reader(f)

        type_map = column_types

        # Sample a few rows to infer column types
        if use_column_types and not type_map:
            sample_size = 10
            sample_rows = [next(reader) for _ in range(sample_size)]
            type_map = infer_column_types(header, sample_rows)

            # Reset file pointer and reinitialize reader and Skip header again if present
            f.seek(0)
            reader = csv.reader(f)
            if has_header:
                next(reader)
        else:
            if use_column_types:
                try:
                    actual, expected = len(header), len(type_map)
                except:
                    raise ValueError(f"Supplied column types column count wrong.  Did you supply a correct type map?  ")
                if actual != expected:
                    raise ValueError(f"Mismatch in actual ({actual}) and expected ({expected}) types column count")


        #Start reading file
        for row_index, rowx in enumerate(reader):
            #Use type map if available
            if use_column_types and type_map:
                row_dict = {
                    col: type_map[col](val) if val != '' else None
                    for col, val in zip(header, rowx)
                }
            else:
                row_dict = {
                    col: val if val != '' else None for col, val in zip(header, rowx)
                }

            #Use filter if available
            if not partial_predicate_fn:
                yield row_dict
            elif partial_predicate_fn(row_index=row_index, row=(row_dict := try_flatten_embedded_json(row_dict, sep="."))):
                yield row_dict


def scan_json_quotes(file, encoding="utf-8"):
    """
    Generator that yields line numbers where quotes might be imbalanced.
    Operates in streaming mode; does not load entire file into memory.

    Yields: -1 if unclosed quote at end & line number of first suspicious

    """
    open_quote = False
    escaped = False
    reported = False
    with open(file, "r", encoding=encoding) as f:
        for line_no, line in enumerate(f, start=1):
            for ch in line:
                if ch == "\\":
                    escaped = not escaped
                elif ch == '"' and not escaped:
                    open_quote = not open_quote
                else:
                    escaped = False
            if open_quote and not reported:
                reported = True
                yield line_no  # Suspicious line, quote not closed

        if open_quote:
            yield -1  # Indicates end-of-file imbalance

def check_json_quotes(file, encoding="utf-8") -> bool:
    """Checks a JSON file for unclosed quotes

    Args:
        file (str): Path to JSON file to be checked for unclosed quotes
        encoding (str, optional): encoding used in file. Defaults to "utf-8".

    Output:
        prints "Unclosed quote may start at line {result[0]}"
    Returns:
        bool: True if ok, False if unclosed Quotes
    Raises:
        ValueError: File ends while still inside a string.
    """
    if (result := list(scan_json_quotes(file, encoding))):
        if len(result)>1:
            print(f"Unclosed quote may start at line {result[0]}")
        raise ValueError("File ends while still inside a string.")
    return not result


def detect_encoding_from_jason_bom(raw: bytes) -> tuple[str, int]:
    """Detect encoding from a jason file.
    The JSON standard requires strict UTF-8 text encoding and does not require a UTF-8 BOM to be embedded
    at the start of the file, however some text applications embedd encoding BOM's irrespective.  This
    routine is a helper routine for the json_reader that detects and analyses a BOM if preseent and returns the encoding
    and number of bytes consumed to allow proper handling by the json readers.

    Args:
        raw (bytes):Up to 4 bytes extracted from the start of a file.

    Returns:
        tuple[str, int]: a tuple containing (encoding , number of bytes used).  The first term is the
                         encoding found and the second is the number of bytes used in the encoding that
                         should be skipped at the start of the file.  It defaults to "UTF-8", 0 if no
                         encoding found.
    """
    if raw.startswith(codecs.BOM_UTF8):
        return "utf-8-sig", len(codecs.BOM_UTF8)
    elif raw.startswith(codecs.BOM_UTF16_LE):
        return "utf-16", len(codecs.BOM_UTF16_LE)
    elif raw.startswith(codecs.BOM_UTF16_BE):
        return "utf-16", len(codecs.BOM_UTF16_BE)
    elif raw.startswith(codecs.BOM_UTF32_LE):
        return "utf-32", len(codecs.BOM_UTF32_LE)
    elif raw.startswith(codecs.BOM_UTF32_BE):
        return "utf-32", len(codecs.BOM_UTF32_BE)
    else:
        return "utf-8", 0  # <- Safer fallback for JSON


def json_reader(file, partial_predicate_fn=None, encoding="utf-8", json_type="auto"):
    """
    Generator-based reader for large JSON files, optionally filtered via a predicate.
    Supports:
        - auto (detect NDJSON or Standard formats) (json_type="auto")
        - NDJSON (line at a time) (json_type="ndjson")
        - Standard JSON arrays (object at a time) (json_type="stnd")

    Optional settings:
    If encoding is set to "check", the first 4 bytes of the file will be read and checked
    for a Byte Order Mark (BOM). If detected, the encoding will be updated accordingly.
    This supports robust handling of UTF BOMs commonly introduced by some tools (e.g. Excel),
    but is safer when explicitly invoked. Unlike CSVs, JSON parsers expect strict UTF compliance
    and may misinterpret BOMs unless handled correctly. The JSON standard allows ONLY UTF-8, so
    if another encoding is found AND we are reading standard JSON the file will be rejected.  If
    reading NDJSON the file with UTF-8, UTF-16 or UTF-32 will be accepted and read correctly even though
    these last two are not standards compliant.  This difference in handling arises because of the
    different ways we have to parse the two different file structures and for the NDJSON files we are
    able to handle the non standard encoding format simply. Neither file type should be in anything other than
    UTF-8 to be JSON compliant.

    If a partial_predicate_fn is provided, it should be a function expecting two parameters:
    (row_index=row_index, row=row_dict), where row_index is an int starting at 0, and
    row is a dictionary parsed from a JSON object. The function should return True to include
    the row or False to skip it.

    Args:
        file (str): Path to the JSON file.
        partial_predicate_fn (function, optional): Predicate function. Defaults to None.
        encoding (str, optional): Text encoding ("utf-8", "utf-8-sig", "check", etc.). Defaults to "utf-8".
        json_type (str, optional): Whether to read as NDJSON ("ndjson") or standard JSON array ("stnd"). Defaults to "auto".

    Yields:
        dict: Dictionary object for each row/outer object.

    Raises:
        ValueError: Unable to determine JSON format from leading characters.
        ValueError: Unrecognised JSON type.
        ValueError: Standard JSON streaming requires UTF-8 encoding. Got: {encoding}
    """

    skip_bytes = 0
    if encoding.lower() == "check":
        with open(file, "rb") as f:
            raw = f.read(4)
            encoding, skip_bytes = detect_encoding_from_jason_bom(raw)

    # Auto-detect format based on first non-whitespace character
    if json_type.lower() == "auto":
        with open(file, "r", encoding=encoding) as f:
            first_char = next((ch for ch in f.read(1024) if not ch.isspace()), None)
            match first_char:
                case "[":         #Standard JSON
                    json_type = "stnd"
                case "{":       #NDJSON
                    json_type = "ndjson"
                case _ :
                    raise ValueError("Unable to determine JSON format from leading characters.")

    match json_type.lower():
        case "stnd" :
            jstndrd = True
        case "ndjson" :
            jstndrd  = False
        case _ :
            raise ValueError("Unrecognised JSON type.")

    # Begin reading
    if jstndrd:
        # Binary mode for ijson
        # JSON standard requires UTF-8, if we need to expand to UTF 16/32
        # we will need tp convert to a text stream using codecs.getreader(encoding) and wrap the
        # stream in BytesIO.  This would not match the current JSON standard however
        if encoding.lower() not in ("utf-8", "utf-8-sig"):
            raise ValueError(f"Standard JSON streaming requires UTF-8 encoding. Got: {encoding}")

        with open(file, "rb") as f:
            if skip_bytes:
                f.seek(skip_bytes)
            # Standard JSON array mode (streamed via ijson)
            for row_index, row in enumerate(ijson.items(f, 'item')):
                if not partial_predicate_fn or partial_predicate_fn(row_index=row_index, row=row):
                    yield row
    else:
        # Text mode for NDJSON
        with open(file, "r", encoding=encoding) as f:
            for row_index, line in enumerate(f):
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not partial_predicate_fn or partial_predicate_fn(row_index=row_index, row=row):
                    yield row

def recursive_defaultdict():
    return defaultdict(recursive_defaultdict)


def dictify(d):
    """
    Recursively converts defaultdicts to regular dicts.
    """
    if isinstance(d, defaultdict):
        return {k: dictify(v) for k, v in d.items()}
    elif isinstance(d, list):
        return [dictify(item) for item in d]
    return d

def unflatten_dict(d, sep='.'):
    """
    Reconstructs nested dictionaries from flattened ones using dotted and bracketed keys.
    Dict branches use recursive defaultdicts; arrays remain regular lists.
    Supports keys like 'phones[0].type' for arrays of objects.

    Example inputs:
    Example1:
        {"meta.month": "Jan"} -> {"meta": {"month": "Jan"}}

    Example2:
        {
            "ID": "001",
            "name": "Alice",
            "region.loc": "NSW",
            "region.code": "101",
            "phones[0].type": "mobile",
            "phones[0].number": "123-456",
            "phones[1].type": "home",
            "phones[1].number": "987-654"
        }

    Example3:
        {
            "ID": "001",
            "name": "Alice",
            "region.loc": "NSW",
            "region.code": "101",
            "phones": "[{'type': 'mobile', 'number': '123-456'}, {'type': 'home', 'number': '987-654'}]"
        }

    Example4:
        {
            "ID": "001",
            "name": "Alice",
            "region": "{ ""loc"": ""NSW"" }, ""code"": ""101"" }",
            "phones": "[{""type"": ""mobile"", ""number"": ""123-456""}, {""type"": ""home"", ""number"": ""987-654""}]"
        }

    Output:
        {
            "ID": "001",
            "name": "Alice",
            "region" : { "loc" : "NSW" , "code" : "101" },
            "phones": [
                {"type": "mobile", "number": "123-456"},
                {"type": "home", "number": "987-654"}
            ]
        }
    """
    result = recursive_defaultdict()
    array_pattern = re.compile(r"(\w+)\[(\d+)\]")

    for flat_key, value in d.items():
        parts = re.split(rf'{re.escape(sep)}', flat_key)
        cursor = result

        for i, part in enumerate(parts):
            array_match = array_pattern.match(part)
            if array_match:
                arr_key, index = array_match.group(1), int(array_match.group(2))

                if arr_key not in cursor or not isinstance(cursor[arr_key], list):
                    cursor[arr_key] = []

                while len(cursor[arr_key]) <= index:
                    cursor[arr_key].append(recursive_defaultdict())

                if i == len(parts) - 1:
                    cursor[arr_key][index] = value
                else:
                    cursor = cursor[arr_key][index]
            else:
                if i == len(parts) - 1:
                    cursor[part] = value
                else:
                    cursor = cursor[part]
    return dictify(result)  # Convert back to regular dicts if needed

def parse_maybe_json(val):
    """
    Attempts to deserialize a field value.
    Tries JSON first, falls back to Python literal if needed.
    """
    if not isinstance(val, str):
        return val  # Already deserialized

    try:
        return json.loads(val)  # Handles proper JSON
    except ValueError:
        try:
            return ast.literal_eval(val)  # Handles Python-style strings
        except Exception:
            return val  # Leave as-is


def csv_to_json(input_csv, output_json, sep=".", unflatten=True, parse_embedded_json=False, **reader_kwargs):
    """
    Converts a CSV file to standard JSON array format using csv_reader().
    Optionally unflattens dotted keys into nested structures and embedded JSON deserialization.
.
    Writes each row incrementally and avoids full memory loading.

    Args:
        input_csv (str): Path to CSV file.
        output_json (str): Path to output JSON file.
        unflatten (bool): Whether to unflatten dotted keys into nested JSON. Default False.
        sep (str): Separator for unflattening (e.g. '.', '__'). Default '.'.
        parse_embedded_json (bool): Whether to deserialize JSON-like strings in fields.
        reader_kwargs: Any optional arguments to pass to csv_reader().
                        Refer to csv_reader() docs for argument options
    """
    first = True
    with open(output_json, "w", encoding="utf-8") as out:
        out.write("[\n")
        for row in csv_reader(input_csv, **reader_kwargs):
            # Try parsing JSON strings in each field
            if parse_embedded_json:
                row = {k: parse_maybe_json(v) for k, v in row.items()}
            if unflatten:
                row = unflatten_dict(row, sep)
            out.write("" if first else ",\n")
            out.write(json.dumps(row, ensure_ascii=False))  # allow unicode
            first = False
        out.write("\n]")


def csv_to_json2(input_csv, output_json, **reader_kwargs):
    """
    Converts a CSV file to standard JSON array format using csv_reader().
    Writes each row incrementally and avoids full memory loading.

    Args:
        input_csv (str): Path to CSV file.
        output_json (str): Path to output JSON file.
        reader_kwargs: Any optional arguments to pass to csv_reader().
                        Refer to csv_reader() docs for argument options
    """
    first = True
    with open(output_json, "w", encoding="utf-8") as out:
        out.write("[\n")
        for row in csv_reader(file=input_csv, **reader_kwargs):
            if not first:
                out.write(",\n")
            out.write(json.dumps(row, ensure_ascii=False))  # allow unicode
            first = False
        out.write("\n]")


def flatten_dict(d, flatten=True, flatten_lists=False, parent_key='', sep='.', override_keys=None, _position=None):
    """
    Flattens a nested dictionary into a flat dict.
    If override_keys is supplied, replaces each computed key with one from the list,
    using traversal order.
    If flatten=True then nested records are expanded into multiple fields using dot notation "field1.field2", else
    nested records are embedded as jason strings.
    If flatten_lists=True then expands arrays into multiple fields using bracket notation: key[index].field else
    arrays are embedded as jason strings
    """
    if _position is None:
        _position = [0]  # Initialize fresh counter per top-level call.  Note the _position value is mutated in recursive
                         # calls so it changes for the entire recursive stack, not just the local instance.

    items = []
    for k, v in d.items():
        full_key = f"{parent_key}{sep}{k}" if parent_key else k
        if flatten and isinstance(v, dict):
            nested = flatten_dict(v, flatten, flatten_lists, full_key, sep=sep, override_keys=override_keys, _position=_position)
            items.extend(nested.items())
        elif isinstance(v, list):
            if flatten_lists:
                for idx, item in enumerate(v):
                    array_key = f"{full_key}[{idx}]"
                    if isinstance(item, dict):
                        nested = flatten_dict(item, flatten, flatten_lists, array_key, sep=sep, override_keys=override_keys, _position=_position)
                        items.extend(nested.items())
                    else:
                        key_out = override_keys[_position[0]] if override_keys else array_key
                        items.append((key_out, item))
                        _position[0] += 1

            else: #not flatten_lists - embedd as jason string
                key_out = override_keys[_position[0]] if override_keys else full_key
                items.append((key_out, json.dumps(v, ensure_ascii=False)))
                _position[0] += 1
        else:
            key_out = override_keys[_position[0]] if override_keys else full_key
            items.append((key_out, v))
            _position[0] += 1
    return dict(items)



def flatten_keys(d, flatten=True, flatten_lists=False, parent_key='', sep='.'):
    """
    Generates flattened key paths from nested dicts.
    Handles arrays with index-based bracket notation: key[index].field
    """
    keys = []
    for k, v in d.items():
        full_key = f"{parent_key}{sep}{k}" if parent_key else k
        if flatten and isinstance(v, dict):
            keys.extend(flatten_keys(v, flatten, flatten_lists, full_key, sep=sep))
        elif flatten_lists and isinstance(v, list):
            for idx, item in enumerate(v):
                array_key = f"{full_key}[{idx}]"
                if isinstance(item, dict):
                    keys.extend(flatten_keys(item, flatten, flatten_lists, array_key, sep=sep))
                else:
                    keys.append(array_key)

        else:
            keys.append(full_key)
    return keys


def json_to_csv(input_json, output_csv, fieldnames=None, flatten=True, flatten_lists=False, sep='.', encoding="utf-8", **reader_kwargs):
    """Convert a very large JSON file to a very large CSV file, flattening all nested json objects
    Converts a JSON file to CSV format using json_reader().
    Supports NDJSON or standard JSON array formats.

    The routine is designed to work on extremely large datasets reading only one data row into memory at a time.

    A key assumption for this routine to work is that the JSON file is regularly structured so that each
    JSon record is a well formed row consistent in structure with the rows before and after it.  The first
    record row is used as the template for column headings for the following rows, so the nesting present
    in later rows should be identical to the nesting of the first row.  If flattened column count of the headings
    differs from the flattened columns or names found in any row, a ValueError will be raised.

    The process of flattening creates new column names from the nested keys separated by sep ("." by default) if
    flatten is True, otherwise the nested records are embedded as jason strings in the previous level field name.
    When flattened the field names become field1.field1_1, field1.field1_2, etc where field1 and field1_n correspond to
    the field names in the jason file. By default lists are left as embedded jason strings unless flatten_lists is
    also set to True, in which case each list member will also be assigned to its own field / column using a naming
    convention of the form field1.field1_2[0], field1.field1_2[1] where field1 & field1_1 & field1_2 correspond to
    the field names in the records of the jason file and [0]..[n] correspond to the ordered positions in the arrays.
    The first jason "row" is used as the template for building these nested and array indexed field name structures
    so that row must be a complete representation of the entire jason file that follows, or there will be some CSV
    records that have values for fields that do not have column names in the final file.  This is why the
    "embedded jason" options are available (by setting flatten=False or flatten_lists=False): if you are not confident
    that you either know the complete structure or can read it off the first jason row in its entirety, you may be
    better leaving either the nested records or the arrays as embedded jason strings.  If flatten is False, flatten_lists
    is ignored as both nested records and lists are being left as embedded jason strings.  Lists are probably much riskier
    to expand than nested records.  Flatten is True and flatten_lists is False by default.

    Note the sep argument identifies the character used to separate fields in the fieldnames created as part of the flattening
    process.  It is "." by default, but if the csv is being imported into a DB it might be preferrable to use a character like
    "_" instead.

    Note that the csv_to_jason() converter handles all of these formats successfully - embedded jason and expanded / flattened
    jason files and reconstructs the jason file therefrom.  If filters / predicates are to be applied to the converted files
    when reading in the csv, a fully flattened csv will be considerable easier for which to write a filter, BUT you could use the
    unflatten_dict() etc helper routines in this library to create on-the-fly field extractors for the filters if necessary.


    Args:
        input_json (str): Path to JSON file.
        output_csv (str): Path to output CSV file.
        flatten (boolean): If true then nested records are expanded with field.field notation, else nested
                           records and arrays/lists are embedd as jason strings in the first level field
        flatten_lists (boolean) : If true & flatten is true then lists are expanded into separate fields using
                           field.field[n] notation as the column names else lists are embedded as jason strings in the
                           corresponding previous level field name
        fieldnames (list, optional): Column names. If None, inferred from first row.
        sep (str): Seperator character used to string nested keys into a flattened field/column name ("." by default)
        encoding (str, optional): Encoding for output file.
        reader_kwargs: Additional args for json_reader() (e.g. json_type, predicate).
                       Refer to json_reader() for argument details.
    Raises:
        ValueError: No rows found in input JSON.
        ValueError: Expected columns does not equal actual columns row:{i}
    """
    def RowWriter( writer, row, row_num, expected_col_count, field_names, inferred_fieldnames ):
        if flatten:
            try:
                writer.writerow(flatrow := flatten_dict(row, flatten, flatten_lists, sep=sep, override_keys=field_names))
            except Exception as e:
                raise ValueError(f"Writerow failed at row {row_num}. Possible column count mismatch: {e}")
            if (len(flatrow) != expected_col_count):
                raise ValueError(f"Expected columns does not equal actual columns row:{row_num}")
        else:
            if not inferred_fieldnames:
                row = dict(zip(field_names, list(row.values())))
            try:
                writer.writerow(row)
            except Exception as e:
                raise ValueError(f"Writerow failed at row {row_num}. Possible column count mismatch: {e}")


    iterator = json_reader(input_json, encoding=encoding, **reader_kwargs)
    try:
        first_row = next(iterator)
    except StopIteration:
        raise ValueError("No rows found in input JSON.")

    # Infer fieldnames if not provided
    if (inferred_fieldnames := not fieldnames):
        fieldnames = flatten_keys(first_row, flatten=flatten, flatten_lists=flatten_lists, sep=sep) if flatten else list(first_row.keys())

    ExpectedCols = len(fieldnames)

    with open(output_csv, "w", newline="", encoding=encoding) as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        RowWriter( writer, first_row, 0, ExpectedCols, fieldnames, inferred_fieldnames )

        for i, row in enumerate(iterator):
            RowWriter( writer, row, i, ExpectedCols, fieldnames, inferred_fieldnames )


def make_json_serializable(obj: Any, use_type_hints: bool = True) -> Any:
    def wrap(value: Any, type_name: str) -> dict:
        return {"__type__": type_name, "value": value} if use_type_hints else value

    if isinstance(obj, dict):
        return {
            str(k): make_json_serializable(v, use_type_hints)
            for k, v in obj.items()
        }
    elif isinstance(obj, list):
        return [make_json_serializable(item, use_type_hints) for item in obj]
    elif isinstance(obj, set):
        return wrap([make_json_serializable(item, use_type_hints) for item in sorted(obj)], "set")
    elif isinstance(obj, tuple):
        return wrap([make_json_serializable(item, use_type_hints) for item in obj], "tuple")
    elif isinstance(obj, Path):
        return wrap(str(obj), "Path")
    elif isinstance(obj, (datetime, date)):
        return wrap(obj.isoformat(), "datetime")
    elif isinstance(obj, bytes):
        return wrap(obj.decode("utf-8", errors="replace"), "bytes")
    elif isinstance(obj, Exception):
        return wrap(str(obj), "Exception")
    else:
        return obj

def save_dict_to_json(data: dict, file: str, save_type_hints: bool = True):
    """
        Save a dictionary from a jason file
        Args:
            data: dict - dictionary to save
            file: str  - file path source as string from which to read
            save_type_hints: bool - Flag to include type hints in jason files
        returns:
            a tuple with (success : boolean, error : string)
        raises:
            all exceptions encountered during read
    """
    filepath = Path(file)
    try:
        serializable_data = make_json_serializable(data, save_type_hints)
        with filepath.open("w", encoding="utf-8") as f:
            json.dump(serializable_data, f, indent=2, ensure_ascii=False)
        return (True, "")
    except Exception as e:
        return (False, f"Error saving JSON: {e}")


TYPE_REGISTRY = {
    "Path": lambda v: Path(v),
    "datetime": lambda v: datetime.fromisoformat(v),
    "bytes": lambda v: v.encode("utf-8"),
    "Exception": lambda v: Exception(v),
    "set": lambda v: set(v),
    "tuple": lambda v: tuple(v),
}

def restore_typed(obj: Any) -> Any:
    if isinstance(obj, dict):
        if "__type__" in obj and "value" in obj:
            type_name = obj["__type__"]
            value = obj["value"]
            converter = TYPE_REGISTRY.get(type_name)
            return converter(value) if converter else value
        return {restore_typed(k): restore_typed(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [restore_typed(item) for item in obj]
    return obj

def load_dict_from_json(file: str, use_type_hints: bool = True) -> dict:
    """
        Load and return a dictionary from a jason file
        Args:
            file: str  - file path source as string from which to read
            use_type_hints: bool - whether to restore typed values from hints

        returns:
            loaded dictionary
        raises:
            all exceptions encountered during read
    """
    filepath= Path(file)
    data=""
    with filepath.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return restore_typed(data) if use_type_hints else data
        




"""
##Example Usage CSV:
#Check file for string imbalance:
for issue in scan_json_quotes("data/jsncsv/input_messy.json"):
    if issue == -1:
        print("File ends while still inside a string.")
    else:
        print(f"Unclosed quote may start at line {issue}")

if check_json_quotes("data/jsncsv/input_JSONTest.json"):
    for line in json_reader("data/jsncsv/input_JSONTest.json", lambda row_index, row: row['Revenue'] > 110000 and row['Month']=="Apr"):
        print(line)


for i in csv_reader("data/jsncsv/comp_data.csv", lambda **kwargs : kwargs["row"]["Revenue"]> 100000, True, None):
    print(i)

def revenue_filter(row_index, row):
    return row["Revenue"] > 100000

for i in csv_reader("data/jsncsv/comp_data.csv", revenue_filter, True, None):
    print(i)

def region_filter(row_index, row):
    return row["Meta.Location.Region"] == 'East' or row.get("Meta.Phones[0].Num") == 12345

for i in csv_reader("data/jsncsv/input_flat_not3.csv", region_filter, True, None):
    print(i)


##Example Usage JSon:
csv_to_json("data/jsncsv/input_comp_data.csv", "data/jsncsv/companies_output.json", has_header=True)
csv_to_json("data/jsncsv/input_flat_input.csv", "data/jsncsv/unflattened_test.json", has_header=True)
csv_to_json("data/jsncsv/input_flat_array_expanded.csv", "data/jsncsv/unflattened_array_exp.json", has_header=True)
csv_to_json("data/jsncsv/input_flat_array.csv", "data/jsncsv/unflattened_array_test.json", parse_embedded_json=True, has_header=True)
csv_to_json("data/jsncsv/input_flat_not3.csv", "data/jsncsv/flat_not_output3_out.json", parse_embedded_json=True, has_header=True)
json_to_csv("data/jsncsv/input_companies.json", "data/jsncsv/comp_output.csv", json_type="stnd") #Flat
json_to_csv("data/jsncsv/input_testnestedjson.json", "data/jsncsv/flat_output.csv", json_type="stnd") #Flattening
json_to_csv("data/jsncsv/input_testnestedarrayjson.json", "data/jsncsv/flat_array_output.csv", json_type="stnd") #Flattening
json_to_csv("data/jsncsv/input_testnestedarrayjson.json", "data/jsncsv/flat_array_output_indexed.csv", json_type="stnd", flatten_lists=True) #Flattening
json_to_csv("data/jsncsv/input_testnestedjson.json", "data/jsncsv/flat_output2.csv", fieldnames=["A","B","C","D","E","F"], json_type="stnd") #Flattening
json_to_csv("data/jsncsv/input_testnestedjson.json", "data/jsncsv/flat_not_output3.csv", flatten=False, json_type="stnd") #Not Flattening
"""