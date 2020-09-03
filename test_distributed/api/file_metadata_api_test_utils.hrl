%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Definitions of macros and records used in file metadata API tests.
%%% @end
%%%-------------------------------------------------------------------
-author("Bartosz Walkowicz").

-ifndef(FILE_METADATA_API_TEST_UTILS_HRL).
-define(FILE_METADATA_API_TEST_UTILS_HRL, 1).


-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/metadata.hrl").


-define(NEW_ID_METADATA_REST_PATH(__FILE_OBJECT_ID, __METADATA_TYPE),
    <<"data/", __FILE_OBJECT_ID/binary, "/metadata/", __METADATA_TYPE/binary>>
).
-define(DEPRECATED_ID_METADATA_REST_PATH(__FILE_OBJECT_ID, __METADATA_TYPE),
    <<"metadata-id/", __METADATA_TYPE/binary, "/", __FILE_OBJECT_ID/binary>>
).
-define(DEPRECATED_PATH_METADATA_REST_PATH(__FILE_PATH, __METADATA_TYPE),
    <<"metadata/", __METADATA_TYPE/binary, __FILE_PATH/binary>>
).


-define(RDF_METADATA_1, <<"<rdf>metadata_1</rdf>">>).
-define(RDF_METADATA_2, <<"<rdf>metadata_2</rdf>">>).
-define(RDF_METADATA_3, <<"<rdf>metadata_3</rdf>">>).
-define(RDF_METADATA_4, <<"<rdf>metadata_4</rdf>">>).

-define(JSON_METADATA_1, #{
    <<"attr1">> => 1,
    <<"attr2">> => #{
        <<"attr21">> => <<"val21">>,
        <<"attr22">> => <<"val22">>
    }
}).
-define(JSON_METADATA_2, <<"metadata">>).
-define(JSON_METADATA_3, #{
    <<"attr2">> => #{
        <<"attr22">> => [1, 2, 3],
        <<"attr23">> => <<"val23">>
    },
    <<"attr3">> => <<"val3">>
}).
-define(JSON_METADATA_4, #{
    <<"attr3">> => #{
        <<"attr31">> => null
    }
}).
-define(JSON_METADATA_5, #{
    <<"attr3">> => #{
        <<"attr32">> => [<<"a">>, <<"b">>, <<"c">>]
    }
}).

-define(MIMETYPE_1, <<"text/plain">>).
-define(MIMETYPE_2, <<"text/javascript">>).

-define(TRANSFER_ENCODING_1, <<"utf-8">>).
-define(TRANSFER_ENCODING_2, <<"base64">>).

-define(CDMI_COMPLETION_STATUS_1, <<"Completed">>).
-define(CDMI_COMPLETION_STATUS_2, <<"Processing">>).

-define(XATTR_1_KEY, <<"custom_xattr1">>).
-define(XATTR_1_VALUE, <<"value1">>).
-define(XATTR_1, #xattr{name = ?XATTR_1_KEY, value = ?XATTR_1_VALUE}).
-define(XATTR_2_KEY, <<"custom_xattr2">>).
-define(XATTR_2_VALUE, <<"value2">>).
-define(XATTR_2, #xattr{name = ?XATTR_2_KEY, value = ?XATTR_2_VALUE}).

-define(ACL_1, [#{
    <<"acetype">> => <<"0x", (integer_to_binary(?allow_mask, 16))/binary>>,
    <<"identifier">> => ?everyone,
    <<"aceflags">> => <<"0x", (integer_to_binary(?no_flags_mask, 16))/binary>>,
    <<"acemask">> => <<"0x", (integer_to_binary(?all_container_perms_mask, 16))/binary>>
}]).
-define(ACL_2, [#{
    <<"acetype">> => <<"0x", (integer_to_binary(?allow_mask, 16))/binary>>,
    <<"identifier">> => ?everyone,
    <<"aceflags">> => <<"0x", (integer_to_binary(?no_flags_mask, 16))/binary>>,
    <<"acemask">> => <<"0x", (integer_to_binary(
        ?read_attributes_mask bor ?read_metadata_mask bor ?read_acl_mask,
        16
    ))/binary>>
}]).
-define(ACL_3, [#{
    <<"acetype">> => <<"0x", (integer_to_binary(?allow_mask, 16))/binary>>,
    <<"identifier">> => ?everyone,
    <<"aceflags">> => <<"0x", (integer_to_binary(?no_flags_mask, 16))/binary>>,
    <<"acemask">> => <<"0x", (integer_to_binary(
        ?read_metadata_mask bor ?read_attributes_mask bor ?read_acl_mask bor
        ?write_metadata_mask bor ?write_attributes_mask bor ?delete_mask bor ?write_acl_mask,
        16
    ))/binary>>
}]).
-define(OWNER_ONLY_ALLOW_ACL, [#{
    <<"acetype">> => <<"0x", (integer_to_binary(?allow_mask, 16))/binary>>,
    <<"identifier">> => ?owner,
    <<"aceflags">> => <<"0x", (integer_to_binary(?no_flags_mask, 16))/binary>>,
    <<"acemask">> => <<"0x", (integer_to_binary(
        ?read_metadata_mask bor ?read_attributes_mask bor ?read_acl_mask bor
        ?write_metadata_mask bor ?write_attributes_mask bor ?delete_mask bor ?write_acl_mask,
        16
    ))/binary>>
}]).

-define(CDMI_XATTRS_KEY, [
    ?ACL_KEY,
    ?MIMETYPE_KEY,
    ?TRANSFER_ENCODING_KEY,
    ?CDMI_COMPLETION_STATUS_KEY
]).
-define(ONEDATA_XATTRS_KEY, [
    ?JSON_METADATA_KEY,
    ?RDF_METADATA_KEY
]).
-define(ALL_XATTRS_KEYS, [
    ?ACL_KEY,
    ?MIMETYPE_KEY,
    ?TRANSFER_ENCODING_KEY,
    ?CDMI_COMPLETION_STATUS_KEY,
    ?JSON_METADATA_KEY,
    ?RDF_METADATA_KEY,
    ?XATTR_1_KEY,
    ?XATTR_2_KEY
]).

-define(ALL_METADATA_SET_1, #{
    ?ACL_KEY => ?ACL_1,
    ?MIMETYPE_KEY => ?MIMETYPE_1,
    ?TRANSFER_ENCODING_KEY => ?TRANSFER_ENCODING_1,
    ?CDMI_COMPLETION_STATUS_KEY => ?CDMI_COMPLETION_STATUS_1,
    ?JSON_METADATA_KEY => ?JSON_METADATA_4,
    ?RDF_METADATA_KEY => ?RDF_METADATA_1,
    ?XATTR_1_KEY => ?XATTR_1_VALUE
}).
-define(ALL_METADATA_SET_2, #{
    ?ACL_KEY => ?ACL_2,
    ?MIMETYPE_KEY => ?MIMETYPE_2,
    ?TRANSFER_ENCODING_KEY => ?TRANSFER_ENCODING_2,
    ?CDMI_COMPLETION_STATUS_KEY => ?CDMI_COMPLETION_STATUS_2,
    ?JSON_METADATA_KEY => ?JSON_METADATA_5,
    ?RDF_METADATA_KEY => ?RDF_METADATA_2,
    ?XATTR_2_KEY => ?XATTR_2_VALUE
}).


-endif.
