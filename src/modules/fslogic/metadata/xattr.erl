%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc API for files' extended attributes.
%%% @end
%%%-------------------------------------------------------------------
-module(xattr).
-author("Tomasz Lichon").

-include("modules/fslogic/fslogic_common.hrl").

%% API
-export([get_by_name/2, get_by_name/3, delete_by_name/2, exists_by_name/2,
    set/5, list/2]).

-type name() :: binary().
-type value() :: custom_metadata:json_term().
-type transfer_encoding() :: binary(). % <<"utf-8">> | <<"base64">>
-type cdmi_completion_status() :: binary(). % <<"Completed">> | <<"Processing">> | <<"Error">>
-type mimetype() :: binary().

-export_type([name/0, value/0, transfer_encoding/0, cdmi_completion_status/0, mimetype/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @equiv get_by_name(FileUuid, XattrName, false).
%% @end
%%--------------------------------------------------------------------
-spec get_by_name(file_ctx:ctx(), xattr:name()) ->
    {ok, value()} | {error, term()}.
get_by_name(FileCtx, XattrName) ->
    get_by_name(FileCtx, XattrName, false).

%%--------------------------------------------------------------------
%% @doc
%% Gets extended attribute with given name
%% @end
%%--------------------------------------------------------------------
-spec get_by_name(file_ctx:ctx(), xattr:name(), boolean()) ->
    {ok, value()} | {error, term()}.
get_by_name(FileCtx0, XattrName, Inherited) ->
    {#document{}, FileCtx} = file_ctx:get_file_doc(FileCtx0), % check if file exists
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    custom_metadata:get_xattr_metadata(FileUuid, XattrName, Inherited).

%%--------------------------------------------------------------------
%% @doc
%% Deletes extended attribute with given name
%% @end
%%--------------------------------------------------------------------
-spec delete_by_name(file_ctx:ctx(), xattr:name()) ->
    ok | {error, term()}.
delete_by_name(FileCtx, XattrName) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    custom_metadata:remove_xattr_metadata(FileUuid, XattrName).

%%--------------------------------------------------------------------
%% @doc
%% Checks existence of extended attribute with given name
%% @end
%%--------------------------------------------------------------------
-spec exists_by_name(file_ctx:ctx(), xattr:name()) -> boolean().
exists_by_name(FileCtx0, XattrName) ->
    {#document{}, FileCtx} = file_ctx:get_file_doc(FileCtx0), % check if file exists
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    custom_metadata:exists_xattr_metadata(FileUuid, XattrName).

%%--------------------------------------------------------------------
%% @doc
%% Sets extended attribute
%% @end
%%--------------------------------------------------------------------
-spec set(file_ctx:ctx(), name(), value(), Create :: boolean(), Replace :: boolean()) ->
    {ok, datastore:key()} | {error, term()}.
set(FileCtx0, XattrName, XattrValue, Create, Replace) ->
    {#document{}, FileCtx} = file_ctx:get_file_doc(FileCtx0), % check if file exists
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    custom_metadata:set_xattr_metadata(FileUuid, SpaceId, XattrName, XattrValue, Create, Replace).

%%--------------------------------------------------------------------
%% @doc
%% Lists names of all extended attributes associated with given file
%% @end
%%--------------------------------------------------------------------
-spec list(file_ctx:ctx(), boolean()) -> {ok, [xattr:name()]} | {error, term()}.
list(FileCtx0, Inherited) ->
    {#document{}, FileCtx} = file_ctx:get_file_doc(FileCtx0), % check if file exists
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    custom_metadata:list_xattr_metadata(FileUuid, Inherited).