%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013, ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: FSLogic request handlers for special files.
%% @end
%% ===================================================================
-module(fslogic_req_special).
-author("Rafal Slota").

-include("veil_modules/dao/dao.hrl").
-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("logging.hrl").

%% API
-export([create_dir/2, get_file_children/3, create_link/2, get_link/1]).

%% ====================================================================
%% API functions
%% ====================================================================

create_dir(FullFileName, Mode) ->
    ?debug("create_dir(FullFileName ~p, Mode: ~p)", [FullFileName, Mode]),

    NewFileName = fslogic_path:basename(FullFileName),
    ParentFileName = fslogic_path:strip_path_leaf(FullFileName),
    {ok, #veil_document{record = #file{}} = ParentDoc} = fslogic_objects:get_file(ParentFileName),

    {ok, UserDoc} = fslogic_objects:get_user(),

    ok = fslogic_perms:check_file_perms(FullFileName, UserDoc, ParentDoc, write),

    {ok, UserId} = fslogic_context:get_user_id(),

    Groups = fslogic_utils:get_group_owner(fslogic_path:get_user_file_name(FullFileName)), %% Get owner group name based on file access path

    FileInit = #file{type = ?DIR_TYPE, name = NewFileName, uid = UserId, gids = Groups, parent = ParentDoc#veil_document.uuid, perms = Mode},
    %% Async *times update
    CTime = vcn_utils:time(),
    File = fslogic_meta:update_meta_attr(FileInit, times, {CTime, CTime, CTime}),

    {Status, TmpAns} = dao_lib:apply(dao_vfs, save_new_file, [FullFileName, File], fslogic_context:get_protocol_version()),
    case {Status, TmpAns} of
        {ok, _} ->
            fslogic_meta:update_parent_ctime(fslogic_path:get_user_file_name(FullFileName), CTime),
            #atom{value = ?VOK};
        {error, file_exists} ->
            #atom{value = ?VEEXIST};
        _BadStatus ->
            lager:error([{mod, ?MODULE}], "Error: can not create dir: ~s, error: ~p", [FullFileName, _BadStatus]),
            #atom{value = ?VEREMOTEIO}
    end.

get_file_children(FullFileName, ROffset, RCount) ->
    ?debug("get_file_children(FullFileName ~p, ROffset: ~p, RCount: ~p)", [FullFileName, ROffset, RCount]),

    UserFilePath = fslogic_path:get_user_file_name(FullFileName),
    TokenizedPath = string:tokens(UserFilePath, "/"),
    {Num, Offset} =
        case {ROffset, TokenizedPath} of
            {0 = Off0, []} -> %% First iteration over "/" dir has to contain "groups" folder, so fetch `num - 1` files instead `num`
                {RCount - 1, Off0};
            {Off1, []} -> %% Next iteration over "/" dir has start one entry earlier, so fetch `num` files starting on `offset - 1`
                {ROffset, Off1 - 1};
            {Off2, _} -> %% Non-root dir -> proceed normally
                {RCount, Off2}
        end,

    {ok, TmpAns} = dao_lib:apply(dao_vfs, list_dir, [FullFileName, Num, Offset], fslogic_context:get_protocol_version()),

    Children = fslogic_utils:create_children_list(TmpAns),
    case {ROffset, TokenizedPath} of
        {0, []}    -> %% When asking about root, add virtual ?GROUPS_BASE_DIR_NAME entry
            #filechildren{child_logic_name = Children ++ [?GROUPS_BASE_DIR_NAME]}; %% Only for offset = 0
        {_, [?GROUPS_BASE_DIR_NAME]} -> %% For group list query ignore DB result and generate list based on user's teams
            Teams = user_logic:get_team_names({dn, fslogic_context:get_user_dn()}),
            {_Head, Tail} = lists:split(min(Offset, length(Teams)), Teams),
            {Ret, _} = lists:split(min(Num, length(Tail)), Tail),
            #filechildren{child_logic_name = Ret};
        _ ->
            #filechildren{child_logic_name = Children}
    end.

create_link(FullFileName, LinkValue) ->
    ?debug("create_link(FullFileName ~p, LinkValue: ~p)", [FullFileName, LinkValue]),

    UserFilePath = fslogic_path:get_user_file_name(FullFileName),

    NewFileName = fslogic_path:basename(FullFileName),
    ParentFileName = fslogic_path:strip_path_leaf(FullFileName),
    {ok, #veil_document{record = #file{}} = ParentDoc} = fslogic_objects:get_file(ParentFileName),

    {ok, UserDoc} = fslogic_objects:get_user(),

    ok = fslogic_perms:check_file_perms(FullFileName, UserDoc, ParentDoc, write),

    {ok, UserId} = fslogic_context:get_user_id(),

    Groups = fslogic_utils:get_group_owner(UserFilePath), %% Get owner group name based on file access path

    LinkDocInit = #file{type = ?LNK_TYPE, name = NewFileName, uid = UserId, gids = Groups, ref_file = LinkValue, parent = ParentDoc#veil_document.uuid},
    CTime = vcn_utils:time(),
    LinkDoc = fslogic_meta:update_meta_attr(LinkDocInit, times, {CTime, CTime, CTime}),

    case dao_lib:apply(dao_vfs, save_new_file, [FullFileName, LinkDoc], fslogic_context:get_protocol_version()) of
        {ok, _} ->
            fslogic_meta:update_parent_ctime(UserFilePath, CTime),
            #atom{value = ?VOK};
        {error, file_exists} ->
            lager:error("Cannot create link - file already exists: ~p", [FullFileName]),
            #atom{value = ?VEEXIST};
        {error, Reason} ->
            lager:error("Cannot save link file (from ~p to ~p) due to error: ~p", [FullFileName, LinkValue, Reason]),
            #atom{value = ?VEREMOTEIO}
    end.

get_link(FullFileName) ->
    ?debug("get_link(FullFileName ~p)", [FullFileName]),

    {ok, #veil_document{record = #file{ref_file = Target}}} = fslogic_objects:get_file(FullFileName),
    #linkinfo{file_logic_name = Target}.

%% ====================================================================
%% Internal functions
%% ====================================================================
