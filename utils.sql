-- ********************************************************************************************
-- General FX utility functions in PL/pgSql (Postgres)
-- Released under the MIT license: http://opensource.org/licenses/mit-license.php by Jon Asher
-- Originally published source at https://gist.github.com/jasher/6026284
-- ********************************************************************************************

CREATE FUNCTION array_contains(p_int_arr integer[], p_value integer) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
BEGIN
  FOR i IN array_lower(p_int_arr, 1)..array_upper(p_int_arr, 1) LOOP
   	-- null array value never matches
      IF p_int_arr[i] Is Null THEN
      	Return false;
      ElsIf p_int_arr[i] = p_value THEN
      	Return true;
      END IF;
	END LOOP;
   -- no match found
   Return false;
END;
$$;


CREATE FUNCTION array_contains(p_str_arr character varying[], p_value integer) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
BEGIN
	FOR i IN array_lower(p_str_arr, 1)..array_upper(p_str_arr, 1) LOOP
   	-- handle nulls separately
      IF p_str_arr[i] Is Null THEN
      	IF p_value Is Null THEN
      		Return true;
          Else
         	Return false;
         END IF;
      ElsIf p_int_arr[i] = p_value THEN
      	Return true;
      END IF;
	END LOOP;
   -- no match found
   Return false;
END;
$$;


CREATE FUNCTION array_to_int_array(p_str_arr character varying[]) RETURNS integer[]
    LANGUAGE plpgsql
    AS $$
/*
Convert a varchar array to an integer array
for each item where value is valid int
*/
DECLARE
    v_item integer;
    v_int_arr integer[];
    i integer;
BEGIN
	If (p_str_arr Is Null) Then
    	Return v_int_arr;
    End If;
    FOR i IN array_lower(p_str_arr, 1)..array_upper(p_str_arr, 1) LOOP
        If (p_str_arr[i] is not null) Then
           v_item = fx.to_integer(p_str_arr[i]);
           If (v_item != 2147483647) Then
              v_int_arr = v_int_arr || v_item;
           End If;
        End If;
    END LOOP;
    Return v_int_arr;
END;
$$;


CREATE FUNCTION array_to_quoted_string(p_array character varying[]) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
-- Return a list of single-quoted items for inclusion
-- in a SQL string
DECLARE
    i integer;
    v_item varchar;
    v_list varchar = '';
BEGIN
    FOR i IN array_lower(p_array, 1)..array_upper(p_array, 1) LOOP
        v_item = Trim(p_array[i]);
        -- if valid item
        If v_item != '' Then
           -- if string not empty, append delimiter
           If v_list != '' Then
              v_list = v_list || ',';
           End If;

           v_list = v_list || quote_literal(v_item);
        End If;
    END LOOP;
    Return v_list;
END;
$$;


CREATE FUNCTION array_to_str_array(p_xml_arr xml[]) RETURNS character varying[]
    LANGUAGE plpgsql
    AS $$
/*
Convert an XML array to a varchar array
for each item where cast value is not null
*/
DECLARE
    v_item varchar;
    v_str_arr varchar[];
    i integer;
BEGIN
	If (p_xml_arr Is Null) Then
    	Return v_str_arr;
	End If;
   FOR i IN array_lower(p_xml_arr, 1)..array_upper(p_xml_arr, 1) LOOP
       If (p_xml_arr[i] Is Not Null) Then
          v_item = fx.to_varchar(p_xml_arr[i]);
          If (v_item Is Not Null) Then
             v_str_arr = v_str_arr || v_item;
          End If;
       End If;
   END LOOP;
   Return v_str_arr;
END;
$$;


CREATE FUNCTION count_nonalpha_chars(p_text character varying) RETURNS integer
    LANGUAGE plpgsql
    AS $$
/* Returns number of extended characters in text string */

DECLARE
    strchar char;
    counter integer;
    i integer;
    strlength integer;
    inputstr varchar;
BEGIN
    inputstr = p_text;
    strlength = length(inputstr);
    counter = 0;
    i = 1;

    WHILE i <= strlength LOOP
        strchar = substring(inputstr FROM i for 1);
        IF ((ascii(strchar) >= 65 AND ascii(strchar) <= 90)
            OR (ascii(strchar) >=97 AND ascii(strchar) <= 122)) THEN
           -- do nothing
          ELSE
           counter = counter + 1;
        END IF;
        i = i + 1;
     END LOOP;

     Return counter;
END
$$;


CREATE FUNCTION ends_with(p_text character varying, p_substr character varying) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
/*
  Determines if text ends with a substring.
  Case insensitive.
*/
DECLARE
   v_pos integer;
BEGIN
   v_pos = Position(Lower(p_substr) in Lower(p_text));
   IF v_pos > 0 THEN
      IF (v_pos + Length(p_substr) - 1) = Length(p_text) THEN
         Return true;
      END IF;
   END IF;

   Return false;
END
$$;


CREATE FUNCTION get_function_defs(p_schema character varying) RETURNS SETOF refcursor
    LANGUAGE plpgsql
    AS $$
DECLARE
   ref_function refcursor;
BEGIN
   OPEN ref_function FOR
        SELECT p.oid, p.proname AS name
              , p.proargtypes, proargnames
              , (fx.get_function_params(p_schema, p.proname::varchar)) As args
              , ds.description, p.prorettype
              , (CASE When Substr(tp.typname,1,3) = 'int' Then 'integer' Else tp.typname End) As rettypename
              , p.proretset, p.probin, p.proisstrict AS strict
              , p.prosrc AS body, l.lanname AS lang
              , u.usename, p.prosecdef, p.provolatile
              , p.proisagg, n.nspname
              , p.proargmodes, p.proallargtypes
       FROM pg_proc p
       LEFT OUTER JOIN pg_description ds ON ds.objoid = p.oid
       LEFT OUTER JOIN pg_type tp ON p.prorettype = tp.oid
       INNER JOIN pg_namespace n ON p.pronamespace = n.oid
       INNER JOIN pg_language l ON l.oid = p.prolang
       LEFT OUTER JOIN pg_user u ON u.usesysid = p.proowner
       WHERE n.nspname = p_schema
       ORDER BY p.proname, n.nspname;

   Return Next ref_function;
END;
$$;


CREATE FUNCTION get_function_params(p_schema character varying, p_function character varying) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
DECLARE
   rec record;
   r integer;
   v_params_arr varchar[];
   v_types varchar;
   v_types_arr integer[];
   v_type integer;
   v_type_name varchar;
   v_params_out varchar = '';

BEGIN
   SELECT proname, p.proargtypes, p.proargnames
          , p.proargmodes, p.proallargtypes INTO rec
   FROM pg_proc p
   INNER JOIN pg_namespace n ON p.pronamespace = n.oid
   WHERE n.nspname = p_schema
         AND proname = p_function;

   v_params_arr = rec.proargnames;   -- string_to_array(v_param_names, ',');
   v_types = array_to_string(rec.proargtypes, ',');

   If Length(v_types) > 0 Then
      v_types_arr = string_to_array(v_types, ',');

      FOR r IN array_lower(v_types_arr, 1)..array_upper(v_types_arr, 1) LOOP
          -- lookup name type
          v_type = v_types_arr[r];
          SELECT typname INTO v_type_name FROM pg_type
          WHERE oid = v_type;
          -- convert all int types to 'integer'
          If Substr(v_type_name, 1, 3) = 'int' Then
             v_type_name = 'integer';
          End If;
          -- add delimiter
          If Length(v_params_out) > 0 Then
             v_params_out = v_params_out || ', ';
          End If;
          -- add param name only when present
          If array_upper(v_params_arr, 1) > 0 Then
             v_params_out = v_params_out || v_params_arr[r] || ' ' || v_type_name;
           Else
             v_params_out = v_params_out || v_type_name;
          End If;
      END LOOP;
   End If;

   Return v_params_out;
END
$$;


CREATE FUNCTION is_distinct(p_val1 character varying, p_val2 character varying) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
/*
Tests 2 strings, val1 and val2, for a different, non-null value in the
second string val2
*/

BEGIN
	If (p_val1 Is Distinct From p_val2) AND (Trim(Coalesce(p_val2, '')) != '') Then
   	Return true;
    Else
    	Return false;
   End If;
END;
$$;


CREATE FUNCTION is_empty(p_val character varying) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
BEGIN
	If Trim(Coalesce(p_val, '')) = '' Then
   	Return true;
    Else
    	Return false;
   End If;
END;
$$;


CREATE FUNCTION is_null_or_empty(p_array numeric[]) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
DECLARE
    i integer;
BEGIN
    IF array_lower(p_array, 1) Is Null THEN
       Return true;
    END IF;

    FOR i IN array_lower(p_array, 1)..array_upper(p_array, 1) LOOP
        If p_array[i] is not null Then
           Return false;
        End If;
    END LOOP;

    -- by this point, no valid array elements found
    Return true;
END;
$$;


CREATE FUNCTION is_null_or_empty(p_array character varying[]) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
DECLARE
    i integer;
    v_item varchar;
BEGIN
    IF array_lower(p_array, 1) Is Null THEN
       Return true;
    END IF;

    FOR i IN array_lower(p_array, 1)..array_upper(p_array, 1) LOOP
        If p_array[i] is not null Then
           v_item = Trim(p_array[i]);
           If Length(v_item) > 0 Then
              Return false;
           End If;
        End If;
    END LOOP;

    -- by this point, no valid array elements found
    Return true;
END;
$$;


CREATE FUNCTION array_add_item(p_list character varying, p_item character varying, p_place character varying) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_list varchar;
BEGIN
    -- if invalid param
    IF (Coalesce(p_item, '') = '') THEN
	   Return p_list;
    END IF;
    -- no duplicates allowed
    IF (fx.unique_text_in_first(p_item, p_list) = '') THEN
       Return p_list;
    END IF;

    v_list = Trim(both ',' from p_list);
    v_list = Trim(both ' ' from v_list);
    -- return new delimited list based on p_place param
    IF Lower(p_place) = 'first' THEN
       Return p_item || ',' || v_list;
     ELSIF Lower(p_place) = 'last' THEN
       Return v_list || ',' || p_item;
     ELSE
       Return v_list || ',' || p_item;
	END IF;
END;
$$;


CREATE FUNCTION remove_dub_space(p_text character varying) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
/* Replace double space chars with a single space char */
DECLARE
   v_outstr varchar = p_text;
BEGIN
   WHILE position('  ' in v_outstr) > 0 LOOP
   	v_outstr = Replace(v_outstr, '  ', ' ');
	END LOOP;
   Return v_outstr;
END
$$;


CREATE FUNCTION remove_end_chars(p_text character varying, p_end_chars character varying) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
DECLARE
	counter integer;
	v_len integer;
   v_char varchar;
   v_text varchar = p_text;
   v_found boolean = true;
BEGIN
	v_len = Length(p_end_chars);
   WHILE v_found LOOP
   	v_found = false;
     	counter = 1;
   	WHILE counter <= v_len LOOP
   		v_char = Substring(p_end_chars FROM counter for 1);
        	IF Substring(v_text FROM Length(v_text) for 1) = v_char THEN
      		v_text = RTrim(v_text, v_char);
            v_found = true;
         END IF;
         counter = counter + 1;
   	END LOOP;
   END LOOP;
   Return v_text;
END;
$$;


CREATE FUNCTION remove_end_text(p_text character varying, p_substr character varying) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
/*
  If present, removes a substr from the end of a larger
  text string.  Case insensitive.
*/
DECLARE
   v_pos integer;
   v_text varchar;
   v_new_text varchar;
BEGIN
   v_text = Rtrim(p_text);
   v_pos = Position(Lower(p_substr) in Lower(v_text));
   IF v_pos > 0 THEN
      IF (v_pos + Length(p_substr) - 1) = Length(v_text) THEN
         v_new_text = Substr(v_text, 1, v_pos-1);
         v_new_text = Rtrim(v_new_text, ', ');
         Return v_new_text;
      END IF;
   END IF;

   Return p_text;
END
$$;



CREATE FUNCTION remove_funky_chars(p_text character varying) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
/* Purpose : removes special characters
             allows a-z, A-Z, 0-9, space, and list of other chars     */
DECLARE
    v_outstr varchar;
    v_allow_chars varchar;
BEGIN
	v_allow_chars = E'-()/\\&_|,';
   v_outstr = fx.remove_nonalpha_chars(p_text, v_allow_chars);

   Return v_outstr;
END
$$;


CREATE FUNCTION remove_nonalpha_chars(p_text character varying) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
/* Purpose : removes special characters
             allows a-z, A-Z, 0-9 and spaces        */
DECLARE
    counter integer;
    v_strchar char;
    v_inputstr varchar;
    v_outstr varchar = '';
BEGIN
    v_inputstr = p_text;
    counter = 1;

    -- remove carriage return, line break, and tab chars
    v_inputstr = Replace(v_inputstr, '/n', '');
    v_inputstr = Replace(v_inputstr, '/r', '');
    v_inputstr = Replace(v_inputstr, '/t', '');

    WHILE counter <= Length(v_inputstr) LOOP
       v_strchar = Substring(v_inputstr FROM counter FOR 1);
       IF ((ascii(v_strchar) >= 65 AND Ascii(v_strchar) <=90) OR (Ascii(v_strchar) >=97 AND Ascii(v_strchar) <=122)
          OR (ascii(v_strchar) >= 48 AND Ascii(v_strchar) <= 57)) THEN
          v_outstr = v_outstr || v_strchar;
        ELSIF (ascii(v_strchar) = 32 OR v_strchar = ' ') THEN
        	 v_outstr = v_outstr || ' ';
       END IF;
       counter = counter+1;
    END LOOP;

    Return v_outstr;
END
$$;


CREATE FUNCTION remove_nonalpha_chars(p_text character varying, p_ignore_chars character varying) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
/* Purpose : removes special characters except those in ignore list
             allows a-z, A-Z, 0-9         */
DECLARE
    counter integer;
    v_strchar char;
    v_inputstr varchar;
    v_regmatch varchar;
    v_outstr varchar = '';
    v_matches varchar[];
    v_found boolean;
BEGIN
    v_inputstr = p_text;
    -- replace space with regex expression for blank char
    --v_regmatch = Replace(Coalesce(p_ignore_chars, ''), ' ', ':BLANK:');
    --v_regmatch = '[' || v_regmatch  || ']';
    counter = 1;

    WHILE counter <= Length(v_inputstr) LOOP
       v_strchar = Substring(v_inputstr FROM counter FOR 1);
       v_found = false;
       IF ((ascii(v_strchar) >= 65 AND Ascii(v_strchar) <=90) OR (Ascii(v_strchar) >=97 AND Ascii(v_strchar) <=122)
          OR (ascii(v_strchar) >= 48 AND Ascii(v_strchar) <= 57)) THEN
          v_found = true;
       END IF;
       IF (Length(Coalesce(p_ignore_chars, '')) > 0) THEN
			--v_matches = regexp_matches(v_strchar, v_regmatch);
         IF Position(v_strchar In p_ignore_chars) > 0 THEN	
         	v_found = true;
      	END IF;
       END IF;

       IF (ascii(v_strchar) = 32 OR v_strchar = ' ') THEN
		 	 v_outstr = v_outstr || ' ';
        -- elseif char found in list of permitted chars
        ELSIF v_found THEN
        	 v_outstr = v_outstr || v_strchar;
        -- else replace invalid char with space to ensure words remain separated
        ELSE
        	 v_outstr = v_outstr || ' ';
       END IF;
       counter = counter+1;
    END LOOP;

    Return v_outstr;
END
$$;


CREATE FUNCTION remove_start_text(p_text character varying, p_substr character varying) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
/*
  If present, removes a substr from the start of a larger
  text string.  Case insensitive.
*/
DECLARE
   v_pos integer;
   v_len integer;
   v_text varchar;
   v_new_text varchar;
BEGIN
   v_text = Ltrim(p_text);
   v_pos = Position(Lower(p_substr) in Lower(v_text));
   IF v_pos = 1 THEN
      v_len = v_pos + Length(p_substr);
      v_new_text = Substr(v_text, v_len, Length(v_text) - v_len  + 1);
      v_new_text = Btrim(v_new_text, ', ');
      Return v_new_text;
   END IF;

   Return p_text;
END
$$;


CREATE FUNCTION replace_extended_chars(p_text character varying, p_replace_char character varying) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
/* Purpose : replace special characters with another char
             allows the  _ , -, a-z, A-Z, 0-9         */
DECLARE
    counter integer;
    v_strchar char;
    v_inputstr varchar;
    v_outstr varchar = '';
    v_char varchar;
BEGIN
    v_inputstr = p_text;
    v_char = p_replace_char;

    -- remove single quotes
    v_inputstr = Replace(v_inputstr, '''', '');
    counter = 1;

    WHILE counter <= Length(v_inputstr) LOOP
       v_strchar = Substring(v_inputstr FROM counter for 1);
       IF ((ascii(v_strchar) >= 65 AND Ascii(v_strchar) <=90) OR (Ascii(v_strchar) >=97 AND Ascii(v_strchar) <=122) OR Ascii(v_strchar)= 45 OR Ascii(v_strchar) = 95       /* hyphen, underscore */
          OR (ascii(v_strchar) >= 48 AND Ascii(v_strchar) <= 57) ) THEN
       	 v_outstr = v_outstr || v_strchar;
        ELSE
          v_outstr = v_outstr || v_char;
       END IF;
       counter = counter+1;
    END LOOP;

    -- remove all contiguous replacement chars
    -- for example, replace '___' with '_'
    -- warning- may have unexpected results in some cases

    WHILE Position((v_char || v_char) in v_outstr) > 0 LOOP
       v_outstr = Replace(v_outstr, (v_char || v_char), v_char);
    END LOOP;

	 If (v_char = '') Then
    	v_outstr = Ltrim(v_outstr, v_char);
    	v_outstr = Rtrim(v_outstr, v_char);
	 End If;

    Return v_outstr;
END
$$;


CREATE FUNCTION replace_html_codes(p_text character varying) RETURNS character varying
    LANGUAGE plpgsql
    AS $_$
DECLARE
	v_inputstr varchar = p_text;
   v_chars varchar[][];
   i integer;
BEGIN
	v_chars = array[['&#32;',' '],['&#33;','!'],['&#34;','""'],['&quot;','""']]
 				|| array[['&#35;','#'],['&#36;','$'],['&#37;','%'],['&#38;','&']]
				|| array[['&amp;','&'],['&#39;',''''],['&40;','('],['&#41;',')']]
				|| array[['&#42;','*'],['&#43;','+'],['&44;',','],['&#45;','-']]
				|| array[['&#46;','.'],['&#47;','/'],['&58;',':'],['&#59;',';']]
				|| array[['&#60;','<'],['&#61;','='],['&62;','>'],['&#63;','?']]
				|| array[['&#64;','@'],['&#91;','['],['&92;','\\'],['&#93;',']']]
				|| array[['&#94;','^'],['&#95;','_'],['&96;',''''],['&#123;','{']]
				|| array[['&#124;','|'],['&#125;','}'],['&126;','~'],['&lt;','<']]
            || ARRAY[['&gt;','>'],['&amp;','&'],['&quot;','"'],['&apos;','''']];

   FOR i IN array_lower(v_chars, 1)..array_upper(v_chars, 1) LOOP
		v_inputstr = Replace(v_inputstr, v_chars[i][1], v_chars[i][2]);
   END LOOP;

   v_inputstr = regexp_replace(v_inputstr, '&#..;', '');
   v_inputstr = regexp_replace(v_inputstr, '&#...;', '');

   Return v_inputstr;
END;
$_$;


--
-- Name: to_int(character varying); Type: FUNCTION; Schema: fx; Owner: -
--

CREATE FUNCTION to_int(p_val character varying) RETURNS integer
    LANGUAGE plpgsql
    AS $$
BEGIN
  -- Cast value to int; on failure return 0
   BEGIN
      RETURN Cast(p_val As integer);
    EXCEPTION WHEN Others THEN
      RETURN 0;
   END;

END
$$;


--
-- Name: to_varchar(xml); Type: FUNCTION; Schema: fx; Owner: -
--

CREATE FUNCTION to_varchar(p_val xml) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
BEGIN
  -- Cast value to int; on failure return null
   BEGIN
      RETURN Cast(p_val As varchar);
    EXCEPTION WHEN Others THEN
      RETURN null;
   END;

END
$$;


CREATE FUNCTION trim_clean(p_text character varying) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
BEGIN
    Return Rtrim(p_text, ', ()-');
END;
$$;


CREATE FUNCTION truncate_at_text(p_text character varying, p_break_text character varying) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
/*
   Truncate string at beginning of passed substring.
   Case insensitive. (Similar to truncate_text).
*/
DECLARE
    v_text varchar;
    v_new_text varchar;
    v_pos integer;
BEGIN
    -- look for break off char
    v_text = Rtrim(p_text);
    v_pos = Position(Lower(p_break_text) in Lower(v_text));
    IF v_pos > 0 THEN
       v_new_text = Substr(v_text, 1, v_pos-1);
       v_new_text = fx.trim_clean(v_new_text);
       Return v_new_text;
    END IF;

    Return p_text;

END
$$;


CREATE FUNCTION truncate_post_text(p_text character varying, p_break_text character varying) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
/*
    Truncate a string following the presence of a
    substring.  Case insensitive.
*/
DECLARE
    v_text varchar;
    v_new_text varchar;
    v_pos integer;
BEGIN
    -- look for break off char
    v_text = Rtrim(p_text);
    v_pos = Position(Lower(p_break_text) in Lower(v_text));
    IF v_pos > 0 THEN
       v_new_text = Substr(v_text, 1, (v_pos + Length(p_break_text)-1));
       v_new_text = fx.trim_clean(v_new_text);
       Return v_new_text;
    END IF;

    Return p_text;

END
$$;


CREATE FUNCTION truncate_text(p_text character varying, p_break_char character varying, p_max_length integer) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
/*
  Truncate text at the first instance of a string (break_char).
  If substring not found, break at first space char found.
*/

DECLARE
    v_text varchar;
    v_new_text varchar;
    v_char varchar;
    v_pos integer;
    v_counter integer;

BEGIN
    v_text = Rtrim(p_text);
    -- first, look for break off char when passed
    IF Coalesce(p_break_char, '') <> '' THEN
       v_pos = Position(Lower(p_break_char) in Lower(v_text));
       IF v_pos > 0 And v_pos < p_max_length THEN
          v_new_text = Substr(v_text, 1, v_pos-1);
          v_new_text = fx.trim_clean(v_new_text);
          Return v_new_text;
       END IF;
    END IF;
    -- return full text if max length not reached
    IF Length(p_text) <= p_max_length THEN
        Return p_text;
    END IF;

    -- break char not found before max length, so break string at first space
    v_counter = p_max_length;
    WHILE v_counter > (p_max_length - 10) LOOP
        v_char = Substr(p_text, v_counter, 1);
        IF (v_char = ' ') THEN
           v_new_text = Substr(p_text, 1, v_counter-1);
           v_new_text = fx.trim_clean(v_new_text);
           Return v_new_text;
        END IF;
        v_counter = v_counter - 1;
     END LOOP;

     -- space not found, just break at max length
     v_new_text = Substr(p_text, 1, p_max_length);
     v_new_text = fx.trim_clean(v_new_text);

     Return v_new_text;
END
$$;


CREATE FUNCTION truncate_text(p_text character varying, p_break_char character varying, p_max_length integer, p_break_length integer) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
/*
  Truncate text after a minimum num of chars (break_length) at the position
  of a substring (break_char) in the text.
  If substring not found, truncate text at a maximum length (max_length).
*/

DECLARE
    v_text varchar;
    v_new_text varchar;
    v_char varchar;
    v_pos integer;
    counter integer;

BEGIN
    v_text = Rtrim(p_text);
    -- return full text if max length not reached
    IF Length(p_text) <= p_max_length THEN
        Return p_text;
    END IF;

    -- break at first instance of char
    counter = p_max_length;
    WHILE counter > (p_max_length - p_break_length) LOOP
        v_char = Substr(p_text, counter, 1);
        IF (v_char = p_break_char) THEN
           v_new_text = Substr(p_text, 1, counter-1);
           v_new_text = fx.trim_clean(v_new_text);
           Return v_new_text;
        END IF;
        counter = counter - 1;
     END LOOP;

     -- break char not found, just break at max length
     v_new_text = Substr(p_text, 1, p_max_length);
     v_new_text = fx.trim_clean(v_new_text);
     -- trim before returning
     v_new_text = Ltrim(v_new_text, p_break_char);
     v_new_text = Rtrim(v_new_text, p_break_char);

     Return v_new_text;
END
$$;


CREATE FUNCTION type_max(p_typename character varying) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
DECLARE
	v_type varchar;
BEGIN
	v_type = Lower(Trim(p_typename));
   If v_type = 'int' OR v_type = 'integer' Then
   	Return '2147483647';
    ElsIf v_type = 'bigint' Then
   	Return '9223372036854775807';
    ElsIf v_type = 'smallint' Then
      Return '32767';
   End If;

   Return 0;
END;
$$;


CREATE FUNCTION unique_int_in_first(p_first_list character varying, p_sec_list character varying) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
/* Return the items in list 1, but not in list 2 */
DECLARE
    v_item integer;
    v_item2 integer;
    v_unique_list varchar = '';
    found bool;
    i integer;
    j integer;
    v_first_arr varchar[];
    v_sec_arr varchar[];
    delim varchar = ',';

BEGIN
    -- if either list is empty, return first list
    If Coalesce(p_first_list, '') = '' OR Coalesce(p_sec_list, '') = '' Then
       Return p_first_list;
    End If;

    v_first_arr = string_to_array(p_first_list, delim);
    v_sec_arr = string_to_array(p_sec_list, delim);

    FOR i IN array_lower(v_first_arr, 1)..array_upper(v_first_arr, 1) LOOP
        If (v_first_arr[i] is not null) Then
           v_item = fx.to_int(v_first_arr[i]);
           If (v_item > 0) Then
              found = false;
              -- begin match loop
              FOR j IN array_lower(v_sec_arr, 1)..array_upper(v_sec_arr, 1) LOOP
                  If (v_sec_arr[j] is not null) Then
                     v_item2 = fx.to_int(v_sec_arr[j]);
                     If (v_item2 > 0) Then
                        If (v_item = v_item2) Then
                           found = true;
                           Exit;
                        End If;
                     End If;
                  End If;
              END LOOP;
              -- finished looping- match found?
              If Not found Then
                 If Length(v_unique_list) > 0 Then
                    v_unique_list = v_unique_list || ',';
                 End If;
                 v_unique_list = v_unique_list || v_item::varchar;
              End If;
           End If;
        End If;
    END LOOP;

    Return v_unique_list;
END
$$;


CREATE FUNCTION unique_text_in_first(p_first_list character varying, p_sec_list character varying) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
/* Return the items in list 1, but not in list 2 */
DECLARE
    v_item varchar;
    v_item2 varchar;
    v_unique_list varchar = '';
    found bool;
    i integer;
    j integer;
    v_first_arr varchar[];
    v_sec_arr varchar[];
    delim varchar = ',';

BEGIN
    -- if either list is empty, return first list
    If Coalesce(p_first_list, '') = '' OR Coalesce(p_sec_list, '') = '' Then
       Return p_first_list;
    End If;

    v_first_arr = string_to_array(p_first_list, delim);
    v_sec_arr = string_to_array(p_sec_list, delim);

    FOR i IN array_lower(v_first_arr, 1)..array_upper(v_first_arr, 1) LOOP
        If (v_first_arr[i] is not null) Then
           v_item = Coalesce(v_first_arr[i], '');
           If (v_item != '') Then
              found = false;
              -- begin match loop
              FOR j IN array_lower(v_sec_arr, 1)..array_upper(v_sec_arr, 1) LOOP
                  If (v_sec_arr[j] is not null) Then
                     v_item2 = Coalesce(v_sec_arr[j], '');
                     If (v_item2 != '') Then
                        If (v_item = v_item2) Then
                           found = true;
                           Exit;
                        End If;
                     End If;
                  End If;
              END LOOP;
              -- finished looping- match found?
              If Not found Then
                 If Length(v_unique_list) > 0 Then
                    v_unique_list = v_unique_list || ',';
                 End If;
                 v_unique_list = v_unique_list || v_item::varchar;
              End If;
           End If;
        End If;
    END LOOP;

    Return v_unique_list;
END
$$;


CREATE FUNCTION url_get_base(p_url character varying) RETURNS character varying
    LANGUAGE plpgsql
    AS $_$
/*
Returns a URL excluding the scheme (removes http or ftp)
*/
DECLARE
	v_url varchar;
BEGIN
	v_url = regexp_replace(p_url, E'((^http|https|ftp):\/\/(www\.)?)', '');
   Return regexp_replace(v_url, E'\/$', '');
END;
$_$;


CREATE FUNCTION url_get_domain(p_url character varying) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
/*
Returns a top level domain name such as 'example.com'
*/
DECLARE
	v_match varchar[];
BEGIN
	v_match = regexp_matches(p_url, E'[a-zA-Z0-9\\-_]+\.(?:com|net|org|info)');
   If v_match Is Not Null Then
   	If array_upper(v_match, 1) > 0 Then
      	Return v_match[1];
      End If;
   End If;
   -- else, not match found
	Return null;
END;
$$;


CREATE FUNCTION xml_forest(p_content character varying, p_name character varying) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
BEGIN
   Return '<' || p_name || '>' || p_content || '</' || p_name || '>';
END;
$$;


CREATE FUNCTION xml_forest(p_content_arr character varying[], p_name character varying) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
DECLARE
	i integer;
	v_content varchar = '';
BEGIN
	IF fx.is_null_or_empty(p_content_arr) THEN
   	Return '';
   END IF;
	FOR i IN array_lower(p_content_arr, 1)..array_upper(p_content_arr, 1) LOOP
  		v_content = v_content || Coalesce(p_content_arr[i], '');
	END LOOP;

   Return '<' || p_name || '>' || v_content || '</' || p_name || '>';
END;
$$;
