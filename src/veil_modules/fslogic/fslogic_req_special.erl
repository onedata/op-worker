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
-include_lib("ctool/include/logging.hrl").

%% API
-export([create_dir/2, get_file_children/4, create_link/2, get_link/1]).

%% ====================================================================
%% API functions
%% ====================================================================


%% create_dir/2
%% ====================================================================
%% @doc Creates new directory.
%% @end
-spec create_dir(FullFileName :: string(), Mode :: non_neg_integer()) ->
    #atom{} | no_return().
%% ====================================================================
create_dir(FullFileName, Mode) ->
    ?debug("create_dir(FullFileName ~p, Mode: ~p)", [FullFileName, Mode]),

    NewFileName = fslogic_path:basename(FullFileName),
    ParentFileName = fslogic_path:strip_path_leaf(FullFileName),
    {ok, #veil_document{record = #file{}} = ParentDoc} = fslogic_objects:get_file(ParentFileName),

    {ok, UserDoc} = fslogic_objects:get_user(),

    ok = fslogic_perms:check_file_perms(FullFileName, UserDoc, ParentDoc, write),

    {ok, UserId} = fslogic_context:get_user_id(),

    FileInit = #file{type = ?DIR_TYPE, name = NewFileName, uid = UserId, parent = ParentDoc#veil_document.uuid, perms = Mode},
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
            ?error("Can not create dir: ~s, error: ~p", [FullFileName, _BadStatus]),
            #atom{value = ?VEREMOTEIO}
    end.


%% get_file_children/3
%% ====================================================================
%% @doc Lists directory. Start with ROffset entity and limit returned list to RCount size.
%% @end
-spec get_file_children(FullFileName :: string(), UserPathTokens :: [string()], ROffset :: non_neg_integer(), RCount :: non_neg_integer()) ->
    #filechildren{} | no_return().
%% ====================================================================
get_file_children(FullFileName, UserPathTokens, ROffset, RCount) ->
    ?debug("get_file_children(FullFileName ~p, ROffset: ~p, RCount: ~p)", [FullFileName, ROffset, RCount]),

    TokenizedPath = UserPathTokens,

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
        {0, []}    -> %% When asking about root, add virtual ?SPACES_BASE_DIR_NAME entry
            #filechildren{child_logic_name = Children ++ [?SPACES_BASE_DIR_NAME]}; %% Only for offset = 0
        {_, [?SPACES_BASE_DIR_NAME]} -> %% For group list query ignore DB result and generate list based on user's teams

            {ok, UserDoc} = fslogic_objects:get_user(),
            Teams = user_logic:get_space_names(UserDoc),
            {_Head, Tail} = lists:split(min(Offset, length(Teams)), Teams),
            {Ret, _} = lists:split(min(Num, length(Tail)), Tail),
            #filechildren{child_logic_name = Ret};
        Other ->
            #filechildren{child_logic_name = Children}
    end.


%% create_link/2
%% ====================================================================
%% @doc Creates new symbolic link.
%% @end
-spec create_link(FullFileName :: string(), LinkValue :: string()) ->
    #atom{} | no_return().
%% ====================================================================
create_link(FullFileName, LinkValue) ->
    ?debug("create_link(FullFileName ~p, LinkValue: ~p)", [FullFileName, LinkValue]),

    UserFilePath = fslogic_path:get_user_file_name(FullFileName),

    NewFileName = fslogic_path:basename(FullFileName),
    ParentFileName = fslogic_path:strip_path_leaf(FullFileName),
    {ok, #veil_document{record = #file{}} = ParentDoc} = fslogic_objects:get_file(ParentFileName),

    {ok, UserDoc} = fslogic_objects:get_user(),

    ok = fslogic_perms:check_file_perms(FullFileName, UserDoc, ParentDoc, write),

    {ok, UserId} = fslogic_context:get_user_id(),

    LinkDocInit = #file{type = ?LNK_TYPE, name = NewFileName, uid = UserId, ref_file = LinkValue, parent = ParentDoc#veil_document.uuid},
    CTime = vcn_utils:time(),
    LinkDoc = fslogic_meta:update_meta_attr(LinkDocInit, times, {CTime, CTime, CTime}),

    case dao_lib:apply(dao_vfs, save_new_file, [FullFileName, LinkDoc], fslogic_context:get_protocol_version()) of
        {ok, _} ->
            fslogic_meta:update_parent_ctime(UserFilePath, CTime),
            #atom{value = ?VOK};
        {error, file_exists} ->
            ?error("Cannot create link - file already exists: ~p", [FullFileName]),
            #atom{value = ?VEEXIST};
        {error, Reason} ->
            ?error("Cannot save link file (from ~p to ~p) due to error: ~p", [FullFileName, LinkValue, Reason]),
            #atom{value = ?VEREMOTEIO}
    end.


%% get_link/1
%% ====================================================================
%% @doc Gets value of symbolic link.
%% @end
-spec get_link(FullFileName :: string()) ->
    #linkinfo{} | no_return().
%% ====================================================================
get_link(FullFileName) ->
    ?debug("get_link(FullFileName ~p)", [FullFileName]),

    {ok, #veil_document{record = #file{ref_file = Target}}} = fslogic_objects:get_file(FullFileName),
    #linkinfo{file_logic_name = Target}.

%% ====================================================================
%% Internal functions
%% ====================================================================
