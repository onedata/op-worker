%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This is a helper module for luma_users_cache module.
%%% It encapsulates #credentials{} record.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_user).
-author("Jakub Kudzia").

%% API
-export([new/2, get_storage_credentials/1, get_display_uid/1]).

-record(credentials, {
    % credentials used to perform operations on behalf of the user on storage
    storage_credentials :: luma:storage_credentials(),
    % optional value for overriding uid returned
    % in #file_attr{} record for get_file_attr operation
    display_uid :: undefined | luma:uid()
}).

-type credentials() ::  #credentials{}.
-export_type([credentials/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec new(luma:storage_credentials(), luma:uid()) -> credentials().
new(StorageCredentials, DisplayUid) ->
    #credentials{
        storage_credentials = StorageCredentials,
        display_uid = DisplayUid
    }.

-spec get_storage_credentials(credentials()) -> luma:storage_credentials().
get_storage_credentials(#credentials{storage_credentials = Credentials}) ->
    Credentials.

-spec get_display_uid(credentials()) -> luma:uid().
get_display_uid(#credentials{display_uid = DisplayUid}) ->
    DisplayUid.

