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

-define(ONEDATA_PREFIX, <<"onedata_">>).
-define(ONEDATA_PREFIX_STR, "onedata_").
-define(CDMI_PREFIX, <<"cdmi_">>).
-define(CDMI_PREFIX_STR, "cdmi_").

-define(JSON_METADATA_KEY, <<?ONEDATA_PREFIX_STR, "json">>).
-define(RDF_METADATA_KEY, <<?ONEDATA_PREFIX_STR, "rdf">>).
-define(METADATA_INTERNAL_PREFIXES, [?ONEDATA_PREFIX, ?CDMI_PREFIX]).
