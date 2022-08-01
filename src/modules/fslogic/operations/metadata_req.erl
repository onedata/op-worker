%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016-2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing requests operating on file's
%%% metadata.
%%% @end
%%%--------------------------------------------------------------------
-module(metadata_req).
-author("Tomasz Lichon").

-include("modules/fslogic/metadata.hrl").
-include("proto/oneprovider/provider_messages.hrl").

%% API
-export([get_metadata/5, set_metadata/7, remove_metadata/3]).


%%%===================================================================
%%% API
%%%===================================================================


-spec get_metadata(
    user_ctx:ctx(),
    file_ctx:ctx(),
    custom_metadata:type(),
    custom_metadata:query(),
    Inherited :: boolean()
) ->
    {ok, custom_metadata:value()} | errors:error().
get_metadata(UserCtx, FileCtx0, Type, Query, Inherited) ->
    FileCtx1 = file_ctx:assert_file_exists(FileCtx0),

    Result = case Type of
        json -> json_metadata:get(UserCtx, FileCtx1, Query, Inherited);
        rdf -> xattr:get(UserCtx, FileCtx1, ?RDF_METADATA_KEY, Inherited)
    end,
    case Result of
        {ok, Value} -> {ok, Value};
        {error, not_found} -> ?ERROR_POSIX(?ENOATTR)
    end.


-spec set_metadata(
    user_ctx:ctx(),
    file_ctx:ctx(),
    custom_metadata:type(),
    custom_metadata:value(),
    custom_metadata:query(),
    Create :: boolean(),
    Replace :: boolean()
) ->
    ok.
set_metadata(UserCtx, FileCtx0, json, Value, Query, Create, Replace) ->
    file_ctx:assert_not_trash_dir_const(FileCtx0),
    FileCtx1 = file_ctx:assert_file_exists(FileCtx0),
    {ok, _} = json_metadata:set(UserCtx, FileCtx1, Value, Query, Create, Replace),
    ok;
set_metadata(UserCtx, FileCtx0, rdf, Value, _, Create, Replace) ->
    file_ctx:assert_not_trash_dir_const(FileCtx0),
    FileCtx1 = file_ctx:assert_file_exists(FileCtx0),
    {ok, _} = xattr:set(UserCtx, FileCtx1, ?RDF_METADATA_KEY, Value, Create, Replace),
    ok.


-spec remove_metadata(user_ctx:ctx(), file_ctx:ctx(), custom_metadata:type()) ->
    ok.
remove_metadata(UserCtx, FileCtx0, json) ->
    file_ctx:assert_not_trash_dir_const(FileCtx0),
    FileCtx1 = file_ctx:assert_file_exists(FileCtx0),
    ok = json_metadata:remove(UserCtx, FileCtx1);
remove_metadata(UserCtx, FileCtx0, rdf) ->
    file_ctx:assert_not_trash_dir_const(FileCtx0),
    FileCtx1 = file_ctx:assert_file_exists(FileCtx0),
    ok = xattr:remove(UserCtx, FileCtx1, ?RDF_METADATA_KEY).
