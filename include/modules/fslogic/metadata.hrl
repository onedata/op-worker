%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Common definitions and macros for metadata
%%% @end
%%%-------------------------------------------------------------------
-author("Lukasz Opiola").

-include_lib("ctool/include/posix/acl.hrl").

-define(ONEDATA_PREFIX, <<"onedata_">>).
-define(ONEDATA_PREFIX_STR, "onedata_").
-define(CDMI_PREFIX, <<"cdmi_">>).
-define(CDMI_PREFIX_STR, "cdmi_").

%% Keys of special cdmi attrs
-define(ACL_KEY, ?ACL_XATTR_NAME).
-define(MIMETYPE_KEY, <<?CDMI_PREFIX_STR, "mimetype">>).
-define(TRANSFER_ENCODING_KEY, <<?CDMI_PREFIX_STR, "valuetransferencoding">>).
-define(CDMI_COMPLETION_STATUS_KEY, <<?CDMI_PREFIX_STR, "completion_status">>).

%% Keys of special onedata attrs
-define(JSON_METADATA_KEY, <<?ONEDATA_PREFIX_STR, "json">>).
-define(RDF_METADATA_KEY, <<?ONEDATA_PREFIX_STR, "rdf">>).

-define(METADATA_INTERNAL_PREFIXES, [?ONEDATA_PREFIX, ?CDMI_PREFIX]).
