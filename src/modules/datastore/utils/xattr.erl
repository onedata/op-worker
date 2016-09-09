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

%% API
-export([get_by_name/2, get_by_name/3, delete_by_name/2, exists_by_name/2, save/3, list/1, list/2]).

-type name() :: binary().
-type value() :: binary().
-type transfer_encoding() :: binary(). % <<"utf-8">> | <<"base64">>
-type cdmi_completion_status() :: binary(). % <<"Completed">> | <<"Processing">> | <<"Error">>
-type mimetype() :: binary().

-export_type([name/0, value/0, transfer_encoding/0, cdmi_completion_status/0, mimetype/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @equiv get_by_name(FileUuid, XattrName, false).
%%--------------------------------------------------------------------
-spec get_by_name(file_meta:uuid(), xattr:name()) ->
    {ok, value()} | datastore:get_error().
get_by_name(FileUuid, XattrName) ->
    get_by_name(FileUuid, XattrName, false).

%%--------------------------------------------------------------------
%% @doc Gets extended attribute with given name
%%--------------------------------------------------------------------
-spec get_by_name(file_meta:uuid(), xattr:name(), boolean()) ->
    {ok, value()} | datastore:get_error().
get_by_name(FileUuid, XattrName, Inherited) ->
    custom_metadata:get_xattr_metadata(FileUuid, XattrName, Inherited).

%%--------------------------------------------------------------------
%% @doc Deletes extended attribute with given name
%%--------------------------------------------------------------------
-spec delete_by_name(file_meta:uuid(), xattr:name()) ->
    ok | datastore:generic_error().
delete_by_name(FileUuid, XattrName) ->
    custom_metadata:remove_xattr_metadata(FileUuid, XattrName).

%%--------------------------------------------------------------------
%% @doc Checks existence of extended attribute with given name
%%--------------------------------------------------------------------
-spec exists_by_name(file_meta:uuid(), xattr:name()) -> datastore:exists_return().
exists_by_name(FileUuid, XattrName) ->
    custom_metadata:exists_xattr_metadata(FileUuid, XattrName).

%%--------------------------------------------------------------------
%% @doc Saves extended attribute
%%--------------------------------------------------------------------
-spec save(file_meta:uuid(), name(), value()) ->
    {ok, datastore:key()} | datastore:generic_error().
save(FileUuid, XattrName, XattreValue) ->
    custom_metadata:set_xattr_metadata(FileUuid, XattrName, XattreValue).

%%--------------------------------------------------------------------
%% @equiv list(FileUuid, false).
%%--------------------------------------------------------------------
-spec list(file_meta:uuid()) -> {ok, [xattr:name()]} | datastore:generic_error().
list(FileUuid) ->
    list(FileUuid, false).

%%--------------------------------------------------------------------
%% @doc Lists names of all extended attributes associated with given file
%%--------------------------------------------------------------------
-spec list(file_meta:uuid(), boolean()) -> {ok, [xattr:name()]} | datastore:generic_error().
list(FileUuid, Inherited) ->
    custom_metadata:list_xattr_metadata(FileUuid, Inherited).