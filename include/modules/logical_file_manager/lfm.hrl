%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019-2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common definitions and macros for logical file manager.
%%% @end
%%%-------------------------------------------------------------------
-author("Bartosz Walkowicz").

-ifndef(LFM_HRL).
-define(LFM_HRL, 1).

-define(lfm_check(__FunctionCall), lfm:check_result(__FunctionCall)).

-define(PRIVATE_BASIC_ATTRIBUTES, [
    <<"file_id">>, <<"parent_id">>, <<"name">>, <<"mode">>,
    <<"storage_user_id">>, <<"storage_group_id">>,
    <<"atime">>, <<"mtime">>, <<"ctime">>,
    <<"type">>, <<"size">>, <<"shares">>,
    <<"provider_id">>, <<"owner_id">>, <<"hardlinks_count">>, <<"index">>
]).
-define(PUBLIC_BASIC_ATTRIBUTES, [
    <<"file_id">>, <<"parent_id">>, <<"name">>, <<"mode">>,
    <<"atime">>, <<"mtime">>, <<"ctime">>,
    <<"type">>, <<"size">>, <<"shares">>
]).

-record(file_ref, {
    guid :: file_id:file_guid(),
    % Indicates whether the operation should be performed on the symlink itself
    % or on the target file that it points to (in case of symlink guid):
    % 1) `false` - operation should be performed on the symlink itself.
    % 2) `true` - operation should be performed on the target file the symlink points to.
    % 3) `default` - depending on operation the symlink will be resolved (e.g. 'open',
    %                'create', etc.) or not (e.g. 'unlink').
    %                This simulates default UNIX behaviour.
    follow_symlink = default :: false | true | default
}).

-define(FILE_REF(__GUID), #file_ref{guid = __GUID}).
-define(FILE_REF(__GUID, __FOLLOW_SYMLINK), #file_ref{
    guid = __GUID,
    follow_symlink = __FOLLOW_SYMLINK
}).

-endif.
