%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for management of file_meta links.
%%% The assumptions are as follows:
%%%  * for each directory (identified by ParentUuid) in given scope (SpaceId)
%%%    there is a links forest
%%%  * each tree in the forest is assigned to the provider supporting the space
%%%  * the trees are identified by oneprovider:id()
%%%  * provider is allowed to modify only its own tree
%%% @end
%%%-------------------------------------------------------------------
-module(file_meta_links).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_suffix.hrl").
-include("modules/datastore/datastore_runner.hrl").


-export([get/3, add/4, check_and_add/5]).
-export([delete/4, delete_local/4, delete_remote/5]).
-export([list/6, list_whitelisted/4]).
-export([get_trees/1]).
-export([check_name_and_get_conflicting_files/4]).

%% Exported for mocking in CT tests
-export([get_all/2]).


-type forest() :: file_meta:uuid().
-type link_name() :: file_meta:name().
-type link_target() :: file_meta:uuid().
-type link_revision() :: datastore_links:link_rev().
-type scope () :: od_space:id().
-type tree_id() :: od_provider:id().
-type tree_ids() :: datastore_model:tree_ids().
-type internal_link() :: #link{}.
% list of links with the same name
-type group() :: [internal_link()].
-type fold_acc() :: term().
-type fold_fun() :: fun((internal_link(), fold_acc()) -> {ok | stop, fold_acc()} | {error, term()}).
-type link() :: {link_name(), link_target()}.


%% @formatter:off
% Listing options (see datastore_links_iter.erl in cluster_worker for more information about link listing options)
-type offset() :: integer().
-type non_neg_offset() :: non_neg_integer().
-type size() :: non_neg_integer().
-type token() :: datastore_links_iter:token().
-type list_opts() :: #{
    % required keys
    size := size(),
    % optional keys
    token => token() | undefined,
    offset => offset(),
    prev_link_name => link_name(),
    prev_tree_id => od_provider:id()
}.

% Map returned from listing functions, containing information needed to list next batch
-type list_extended_info() :: #{
    token => datastore_links_iter:token(),
    last_name => link_name(),
    last_tree => od_provider:id()
}.
%% @formatter:on

-export_type([link/0, offset/0, non_neg_offset/0, size/0, token/0, list_opts/0, list_extended_info/0]).

-define(CTX, (file_meta:get_ctx())).
-define(CTX(Scope), ?CTX#{scope => Scope}).
-define(LOCAL_TREE_ID, oneprovider:get_id()).
-define(LINK(FileName, FileUuid), {FileName, FileUuid}).

-define(MINIMAL_TREE_ID_SUFFIX_LEN, 4).
-define(DEFAULT_LS_CHUNK_SIZE, application:get_env(?APP_NAME, ls_chunk_size, 5000)).

%%%===================================================================
%%% API
%%%===================================================================

-spec get(forest(), tree_ids(), link_name() | [link_name()]) ->
    {ok, [internal_link()]} | [{ok, [link()]}] | {error, term()}.
get(ParentUuid, TreeIds, FileNames) ->
    % Scope is not passed to this function as it's irrelevant for get operations
    datastore_model:get_links(?CTX, ParentUuid, TreeIds, FileNames).


-spec add(forest(), scope(), link_name(), link_target()) -> ok | {error, term()}.
add(ParentUuid, Scope, FileName, FileUuid) ->
    ?extract_ok(datastore_model:add_links(?CTX(Scope), ParentUuid, ?LOCAL_TREE_ID, ?LINK(FileName, FileUuid))).


-spec check_and_add(forest(), scope(), tree_ids(), link_name(), link_target()) -> ok | {error, term()}.
check_and_add(ParentUuid, Scope, TreesToCheck, FilLieName, FileUuid) ->
    ?extract_ok(datastore_model:check_and_add_links(?CTX(Scope), ParentUuid, ?LOCAL_TREE_ID, TreesToCheck,
        ?LINK(FileName, FileUuid))).


-spec delete(forest(), scope(), link_name(), link_target()) -> ok.
delete(ParentUuid, Scope, FileName, FileUuid) ->
    case get(ParentUuid, all, FileName) of
        {ok, Links} ->
            case lists:filter(fun(#link{target = Uuid}) -> Uuid == FileUuid end, Links) of
                [#link{tree_id = TreeId, name = FileName, rev = Rev}] ->
                    % pass Rev to ensure that link with the same Rev is deleted
                    case oneprovider:is_self(TreeId) of
                        true ->
                            ok = delete_local(ParentUuid, Scope, FileName, Rev);
                        false ->
                            ok = delete_remote(ParentUuid, Scope, TreeId, FileName, Rev)
                    end;
                [] ->
                    ok
            end;
        {error, not_found} ->
            ok
    end.


-spec delete_local(forest(), scope(), link_name(), link_revision()) -> ok.
delete_local(ParentUuid, Scope, FileName, Revision) ->
    ok = datastore_model:delete_links(?CTX(Scope), ParentUuid, ?LOCAL_TREE_ID, {FileName, Revision}).


-spec delete_remote(forest(), scope(), tree_id(), link_name(), link_revision()) -> ok.
delete_remote(ParentUuid, Scope, TreeId, FileName, Revision) ->
    ok = datastore_model:mark_links_deleted(?CTX(Scope), ParentUuid, TreeId, {FileName, Revision}).


-spec list(forest(), offset() | undefined, size(), token() | undefined, link_name() | undefined,
    tree_id() | undefined) -> {ok, [link()], list_extended_info()} | {error, term()}.
list(ParentUuid, Offset, Size, Token, PrevLinkKey, PrevTreeId) ->

    ListOpts = prepare_opts(Offset, Size, Token, PrevLinkKey, PrevTreeId),
    Result = fold(ParentUuid, fun(Link = #link{name = Name}, Acc) ->
        case not (file_meta:is_hidden(Name) orelse file_meta:is_deletion_link(Name)) of
            true -> {ok, [Link | Acc]};
            _ -> {ok, Acc}
        end
    end, [], ListOpts),

    case Result of
        {{ok, ReversedLinks}, NewToken} ->
            prepare_list_result(ReversedLinks, NewToken);
        {ok, ReversedLinks} ->
            prepare_list_result(ReversedLinks, undefined);
        {error, Reason} ->
            {error, Reason}
    end.


-spec list_whitelisted(forest(), non_neg_offset(), size(), [link_name()]) -> {ok, [link()]}.
list_whitelisted(ParentUuid, NonNegOffset, Size, ChildrenWhiteListed) ->
    ValidLinks = lists:flatmap(fun
        ({ok, L}) ->
            L;
        ({error, not_found}) ->
            [];
        ({error, _} = Error) ->
            error(Error)
    end, file_meta_links:get(ParentUuid, all, ChildrenWhiteListed)),

    case NonNegOffset < length(ValidLinks) of
        true ->
            RequestedLinks = lists:sublist(ValidLinks, NonNegOffset + 1, Size),
            {ok, tag_ambiguous(RequestedLinks)};
        false ->
            {ok, []}
    end.


-spec get_trees(forest()) -> {ok, tree_ids()} | {error, term()}.
get_trees(ParentUuid) ->
    datastore_model:get_links_trees(?CTX, ParentUuid).


%%--------------------------------------------------------------------
%% @doc
%% Checks if given file has conflicts with other files on name field.
%% In case of conflict it returns given file with a tag (unless it's
%% link is in local tree, in such case it's not tagged) and conflicting
%% files also with tagged names.
%%%
%% NOTE !!!
%% Sometimes ParentUuid and Name cannot be taken from document as this
%% function may be used as a result of conflict resolving that involve
%% renamed file document.
%% @end
%%--------------------------------------------------------------------
-spec check_name_and_get_conflicting_files(forest(), link_name(), link_target(), od_provider:id()) ->
    ok | {conflicting, TaggedName :: file_meta:name(), Conflicts :: [link()]}.
check_name_and_get_conflicting_files(ParentUuid, FileName, FileUuid, FileProviderId) ->
    case file_meta_links:get_all(ParentUuid, FileName) of
        {ok, [#link{target = FileUuid}]} ->
            ok;
        {ok, []} ->
            ok;
        {ok, Links} ->
            Links2 = case lists:any(fun(#link{tree_id = TreeId}) -> TreeId =:= FileProviderId end, Links) of
                false ->
                    % Link is missing, possible race on dbsync
                    [#link{tree_id = FileProviderId, name = FileName, target = FileUuid} | Links];
                _ ->
                    Links
            end,
            TaggedLinks = tag_ambiguous(Links2),
            {NameAns, OtherFiles} = lists:foldl(fun
                (?LINK(TaggedName, Uuid), {_NameAcc, OtherAcc}) when Uuid =:= FileUuid ->
                    {TaggedName, OtherAcc};
                (Link, {NameAcc, OtherAcc}) ->
                    {NameAcc, [Link | OtherAcc]}
            end, {FileName, []}, TaggedLinks),
            {conflicting, NameAns, OtherFiles};
        {error, _Reason} ->
            ok
    end.

%%%===================================================================
%%% Exported for CT tests
%%%===================================================================

-spec get_all(forest(), link_name()) -> {ok, [internal_link()]} | {error, term()}.
get_all(ParentUuid, Name) ->
    get(ParentUuid, all, Name).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec prepare_opts(offset() | undefined, size() | undefined, token() | undefined,
    link_name() | undefined, tree_id() | undefined) -> list_opts().
prepare_opts(Offset, Size, Token, PrevLinkKey, PrevTreeId)
    when ((is_integer(Size) andalso Size >= 0) orelse (Size =:= undefined))
    % at lease one of below options must be defined so that we know where to start listing
    andalso (Offset /= undefined orelse Token /= undefined orelse PrevLinkKey /= undefined)
->
    validate_starting_opts(Offset, PrevLinkKey, Token),
    Opts1 = #{size => utils:ensure_defined(Size, ?DEFAULT_LS_CHUNK_SIZE)},
    Opts2 = maps_utils:put_if_defined(Opts1, offset, Offset),
    Opts3 = maps_utils:put_if_defined(Opts2, token, Token),
    Opts4 = maps_utils:put_if_defined(Opts3, prev_link_name, PrevLinkKey),
    maps_utils:put_if_defined(Opts4, prev_tree_id, PrevTreeId).


-spec validate_starting_opts(offset() | undefined, link_name() | undefined, token() | undefined) -> ok.
validate_starting_opts(_, _, #link_token{}) ->
    ok;
validate_starting_opts(Offset, undefined, undefined) when Offset >= 0 ->
    ok;
validate_starting_opts(Offset, undefined, undefined) when Offset < 0 ->
    throw(?EINVAL);
    %% TODO VFS-7208 throw(?ERROR_BAD_VALUE_TOO_LOW(offset, 0));
validate_starting_opts(undefined, LastName, undefined) when is_binary(LastName) ->
    ok;
validate_starting_opts(Offset, LastName, undefined) when is_binary(LastName) andalso is_integer(Offset) ->
    ok.


-spec fold(forest(), fold_fun(), fold_acc(), list_opts()) ->
    {ok, fold_acc()} | {{ok, fold_acc()}, datastore_links_iter:token()} | {error, term()}.
fold(ParentUuid, Fun, AccIn, Opts) ->
    datastore_model:fold_links(?CTX, ParentUuid, all, Fun, AccIn, Opts).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Prepares result of list by tagging ambiguous file names and
%% preparing list_extended_info() structure.
%% @end
%%--------------------------------------------------------------------
-spec prepare_list_result([internal_link()], token() | undefined) ->
    {ok, [link()], list_extended_info()}.
prepare_list_result(ReversedLinks, TokenOrUndefined) ->
    ExtendedInfo = case TokenOrUndefined =/= undefined of
        true -> #{token => TokenOrUndefined};
        false -> #{}
    end,
    ExtendedInfo2 = case ReversedLinks of
        [#link{name = Name, tree_id = Tree} | _] ->
            ExtendedInfo#{last_name => Name, last_tree => Tree};
        _ ->
            ExtendedInfo#{last_name => undefined, last_tree => undefined}
    end,
    {ok, tag_ambiguous(lists:reverse(ReversedLinks)), ExtendedInfo2}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds links tree ids suffix to children names with ambiguous names.
%% @end
%%--------------------------------------------------------------------
-spec tag_ambiguous([internal_link()]) -> [link()].
tag_ambiguous([]) ->
    [];
tag_ambiguous(Links) ->
    LocalTreeId = ?LOCAL_TREE_ID,
    Groups = group_by_name(Links),
    lists:foldl(fun
        ([#link{name = Name, target = FileUuid}], ChildrenAcc) ->
            % do not tag the file as it has a unique Name
            [?LINK(Name, FileUuid) | ChildrenAcc];
        (Group, ChildrenAcc) ->
            % files in the Group have the same name
            % we will add suffix which is part of the links tree id to each file
            % from the remote trees so that they can be distinguished
            SuffixLength = calculate_suffix_length(Group),
            lists:foldl(fun(Link = #link{}, ChildrenAcc2) ->
                [tag_remote_files(Link, LocalTreeId, SuffixLength) | ChildrenAcc2]
            end, ChildrenAcc, Group)
    end, [], Groups).


-spec calculate_suffix_length(group()) -> non_neg_integer().
calculate_suffix_length(Group) ->
    RemoteTreeIds = get_remote_tree_ids(Group),
    LongestCommonPrefixLen = binary:longest_common_prefix(RemoteTreeIds),
    max(?MINIMAL_TREE_ID_SUFFIX_LEN, LongestCommonPrefixLen + 1).


-spec get_remote_tree_ids(group()) -> tree_ids().
get_remote_tree_ids(Group) ->
    LocalTreeId = ?LOCAL_TREE_ID,
    lists:filtermap(fun(#link{tree_id = TreeId}) ->
        case TreeId =/= LocalTreeId of
            true -> {true, TreeId};
            false -> false
        end
    end, Group).


-spec group_by_name([internal_link()]) -> [group()].
group_by_name(Links) ->
    {LastGroup, FinalGroupsAcc} = lists:foldl(fun
        (Link = #link{}, {[], GroupsAcc}) ->
            {[Link], GroupsAcc};
        (#link{name = Name, target = Uuid},
            {SpecialLinkGroup = [#link{name = SpecialLinkName, target = SpecialLinkUuid} | _], GroupsAcc}
        ) when Name =:= SpecialLinkName
          andalso Uuid =:= SpecialLinkUuid ->
            % Link has the same Name as link in the SpecialLinkGroup but both links point to the same
            % Uuid which means that these ARE NOT ambiguous files.
            % It's the same, special file which only has many links (created by many providers).
            % This may happen only for special files with predefined Uuids such as space or trash directories.
            % We'll merge duplicated links so that only one is presented.
            {SpecialLinkGroup, GroupsAcc};
        (Link = #link{name = Name}, {CurrentGroup = [#link{name = NameInGroup} | _], GroupsAcc})
            when Name =:= NameInGroup
        ->
            % Link belongs to CurrentGroup.
            {[Link | CurrentGroup], GroupsAcc};
        (Link = #link{}, {CurrentGroup, GroupsAcc}) ->
            % Link does not belong to CurrentGroup.
            % Start a new group.
            {[Link], [CurrentGroup | GroupsAcc]}
    end, {[], []}, Links),
    [LastGroup | FinalGroupsAcc].


-spec tag_remote_files(internal_link(), tree_id(), non_neg_integer()) -> link().
tag_remote_files(#link{name = Name, target = FileUuid, tree_id = LocalTreeId}, LocalTreeId, _) ->
    ?LINK(Name, FileUuid);
tag_remote_files(#link{name = Name, target = FileUuid, tree_id = RemoteTreeId}, _LocalTreeId, SuffixLength) ->
    Suffix = binary_part(RemoteTreeId, 0, min(SuffixLength, byte_size(RemoteTreeId))),
    ?LINK(?CONFLICTING_LOGICAL_FILE_NAME(Name, Suffix), FileUuid).