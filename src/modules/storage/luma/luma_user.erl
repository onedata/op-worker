%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This is a helper module for luma_users_cache module.
%%% It encapsulates #luma_user{} record.
%%%
%%% This record has 2 fields:
%%%  * storage_credentials - this is context of user (helpers:user_ctx())
%%%    passed to helper to perform operations on storage as a given user.
%%%  * display_uid - this field is used to display owner of a file (UID)
%%%    in Oneclient.
%%%
%%% For more info please read the docs of luma.erl and
%%% luma_users_cache.erl modules.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_user).
-author("Jakub Kudzia").

%% API
-export([new/2, get_storage_credentials/1, get_display_uid/1]).

-record(luma_user, {
    % credentials used to perform operations on behalf of the user on storage
    storage_credentials :: luma:storage_credentials(),
    % optional value for overriding uid returned
    % in #file_attr{} record for get_file_attr operation
    display_uid :: luma:uid()
}).

-type entry() ::  #luma_user{}.
-export_type([entry/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec new(luma:storage_credentials(), luma:uid()) -> entry().
new(StorageCredentials, DisplayUid) ->
    #luma_user{
        storage_credentials = StorageCredentials,
        display_uid = DisplayUid
    }.

-spec get_storage_credentials(entry()) -> luma:storage_credentials().
get_storage_credentials(#luma_user{storage_credentials = Credentials}) ->
    Credentials.

-spec get_display_uid(entry()) -> luma:uid().
get_display_uid(#luma_user{display_uid = DisplayUid}) ->
    DisplayUid.

