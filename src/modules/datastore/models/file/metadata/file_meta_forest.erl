%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for management of file_meta datastore links.
%%% The assumptions are as follows:
%%%  * for each directory (identified by ParentUuid) in given scope (SpaceId)
%%%    there is a links forest
%%%  * each tree in the forest is assigned to the provider supporting the space
%%%  * the trees are identified by oneprovider:id()
%%%  * provider is allowed to modify only its own tree
%%% @end
%%%-------------------------------------------------------------------
-module(file_meta_forest).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/fslogic/file_meta_forest.hrl").
-include("modules/fslogic/fslogic_suffix.hrl").
-include("modules/datastore/datastore_runner.hrl").


-export([get/3, get_local_or_remote/4, add/4, check_and_add/6]).
-export([delete/4, delete_local/4, delete_remote/5]).
-export([list/2, list_whitelisted/3]).
-export([get_trees/1]).
-export([check_name_and_get_conflicting_files/6]).

%% Exported for mocking in CT tests
-export([get_all/2]).


% Internal types
-type forest() :: file_meta:uuid().
-type link_name() :: file_meta:name().
-type link_target() :: file_meta:uuid().
-type link_revision() :: datastore_links:link_rev().
-type scope () :: od_space:id().
-type internal_link() :: datastore_links:link().
% list of links with the same name
-type group() :: [internal_link()].
-type fold_acc() :: term().
-type fold_fun() :: fun((internal_link(), fold_acc()) -> {ok | stop, fold_acc()} | {error, term()}).

% Exported types
-type offset() :: integer().
-type limit() :: non_neg_integer().
-type datastore_list_token() :: datastore_links_iter:token().
-type last_name() :: link_name().
-type last_tree() :: tree_id().
-type link() :: {link_name(), link_target()}.
-type tree_id() :: od_provider:id().
-type tree_ids() :: datastore_model:tree_ids().

%% @formatter:off
-type datastore_fold_opts() :: #{
    % required keys
    size := limit(),
    % one of: token, offset, last_name is required so that we know were to start listing
    token => binary(),
    offset => offset(),
    last_name => last_name(),
    % optional keys
    last_tree => last_tree(),
    inclusive => boolean(),
    % * `true`  - when it is not possible to fetch missing links document by remote driver due to e.g. remote provider
    %             being down, such subtree will be ignored and NO error returned;
    % * `false` - in case described above `interrupted_call` error will be returned.
    ignore_missing_links => boolean() % default: false
}.

-type whitelisted_list_opts() :: #{
    size := limit(),
    offset => non_neg_integer(),
    % optional keys
    last_name => last_name()
}.

-type list_opts() :: datastore_fold_opts() | whitelisted_list_opts().

-type list_extended_info() :: #list_extended_info{}.

-export_type([link/0, offset/0, limit/0, datastore_list_token/0, last_name/0, last_tree/0,
    list_opts/0, list_extended_info/0, tree_id/0, tree_ids/0
]).

-define(CTX, (file_meta:get_ctx())).
-define(CTX(Scope), ?CTX#{scope => Scope}).
-define(CTX(Scope, IgnoreInChanges), ?CTX#{scope => Scope, ignore_in_changes => IgnoreInChanges}).
-define(LOCAL_TREE_ID, oneprovider:get_id()).
-define(LINK(FileName, FileUuid), {FileName, FileUuid}).

-define(MINIMAL_TREE_ID_SUFFIX_LEN, 4).

%%%===================================================================
%%% API
%%%===================================================================

-spec get(forest(), tree_ids(), link_name() | [link_name()]) ->
    {ok, [internal_link()]} | [{ok, [internal_link()]} | {error, term()}] | {error, term()}.
get(ParentUuid, TreeIds, FileNames) ->
    % Scope is not passed to this function as it's irrelevant for get operations
    datastore_model:get_links(?CTX, ParentUuid, TreeIds, FileNames).


%%--------------------------------------------------------------------
%% @doc
%% Gets links using context with remote scope set that allows getting link docs from other providers if they are not
%% synced.
%% Warning - should be used only when we know that link forest exists. Otherwise, it will affect performance.
%% @end
%%--------------------------------------------------------------------
-spec get_local_or_remote(forest(), link_name() | [link_name()], od_space:id(), od_space:id() | undefined) ->
    {ok, [internal_link()]} | [{ok, [internal_link()]} | {error, term()}] | {error, term()}.
get_local_or_remote(ParentUuid, FileNames, Scope, RemoteScope) ->
    datastore_model:get_links(file_meta:get_ctx_with_remote_set(Scope, RemoteScope), ParentUuid, all, FileNames).


-spec add(forest(), scope(), link_name(), link_target()) -> ok | {error, term()}.
add(ParentUuid, Scope, FileName, FileUuid) ->
    ?extract_ok(datastore_model:add_links(?CTX(Scope), ParentUuid, ?LOCAL_TREE_ID, ?LINK(FileName, FileUuid))).


-spec check_and_add(forest(), scope(), boolean(), tree_ids(), link_name(), link_target()) -> ok | {error, term()}.
check_and_add(ParentUuid, Scope, IgnoreInChanges, TreesToCheck, FileName, FileUuid) ->
    ?extract_ok(datastore_model:check_and_add_links(?CTX(Scope, IgnoreInChanges), ParentUuid, ?LOCAL_TREE_ID, TreesToCheck,
        ?LINK(FileName, FileUuid))).


-spec delete(forest(), scope(), link_name(), link_target()) -> ok.
delete(ParentUuid, Scope, FileName, FileUuid) ->
    case get(ParentUuid, all, FileName) of
        {ok, Links} ->
            case lists:filter(fun(#link{target = Uuid}) -> Uuid == FileUuid end, Links) of
                [#link{tree_id = TreeId, name = FileName, rev = Rev}] ->
                    % pass Rev to ensure that link with the same Rev is deleted
                    delete_link(TreeId, ParentUuid, Scope, FileName, Rev);
                [_ | _] = Links ->
                    % possible in case of special dirs
                    %% @TODO VFS-11644 - Untangle special dirs and place their logic in one, well-explained place
                    lists:foreach(fun(#link{tree_id = TreeId, name = FileName, rev = Rev}) ->
                        % pass Rev to ensure that link with the same Rev is deleted
                        delete_link(TreeId, ParentUuid, Scope, FileName, Rev)
                    end, Links);
                [] ->
                    ok
            end;
        {error, not_found} ->
            ok
    end.


-spec delete_link(tree_id(), forest(), scope(), link_name(), link_revision()) -> ok.
delete_link(TreeId, ParentUuid, Scope, FileName, Rev) ->
    case oneprovider:is_self(TreeId) of
        true ->
            ok = delete_local(ParentUuid, Scope, FileName, Rev);
        false ->
            ok = delete_remote(ParentUuid, Scope, TreeId, FileName, Rev)
    end.


-spec delete_local(forest(), scope(), link_name(), link_revision()) -> ok.
delete_local(ParentUuid, Scope, FileName, Revision) ->
    ok = datastore_model:delete_links(?CTX(Scope), ParentUuid, ?LOCAL_TREE_ID, {FileName, Revision}).


-spec delete_remote(forest(), scope(), tree_id(), link_name(), link_revision()) -> ok.
delete_remote(ParentUuid, Scope, TreeId, FileName, Revision) ->
    ok = datastore_model:mark_links_deleted(?CTX(Scope), ParentUuid, TreeId, {FileName, Revision}).


-spec list(forest(), datastore_fold_opts()) -> {ok, [link()], datastore_fold_opts()} | {error, term()}.
list(ParentUuid, Opts) ->
    FinalListingOpts = adjust_opts_for_offset(Opts),
    ExpectedSize = maps:get(size, FinalListingOpts),
    Ctx = case maps:get(ignore_missing_links, FinalListingOpts, false) of
        true ->
            ?CTX;
        false ->
            ?CTX#{
                handle_interrupted_call => false,
                writer_interrupted_call_retries => 0,
                links_tree_interrupted_call_retries => 0
            }
    end,
    Result = fold(Ctx, ParentUuid, fun(Link = #link{name = Name}, {ListAcc, _PrevLink, ListedLinksCount}) ->
        case not (file_meta:is_hidden(Name) orelse file_meta:is_deletion_link(Name)) of
            true -> {ok, {[Link | ListAcc], Link, ListedLinksCount + 1}};
            _ -> {ok, {ListAcc, Link, ListedLinksCount + 1}}
        end
    end, {[], undefined, 0}, FinalListingOpts),

    case Result of
        {{ok, {ReversedLinks, LastLink, ListedLinksCount}}, NextToken} ->
            prepare_list_result(lists:reverse(ReversedLinks), NextToken, ListedLinksCount < ExpectedSize,
                LastLink, maps:get(offset, Opts, 0));
        {error, Reason} ->
            {error, Reason}
    end.


-spec list_whitelisted(forest(), whitelisted_list_opts(), [link_name()]) ->
    {ok, [link()], list_extended_info()} | {error, term()}.
list_whitelisted(ParentUuid, Opts, SortedChildrenWhiteList) ->
    Size = maps:get(size, Opts),
    NonNegOffset = maps:get(offset, Opts, 0),
    LastName = maps:get(prev_link_name, Opts, undefined),
    FilteredChildrenWhiteList = case LastName of
        undefined ->
            SortedChildrenWhiteList;
        LastName ->
            lists:dropwhile(fun(Name) -> Name < LastName end, SortedChildrenWhiteList)
    end,
    case get_links_up_to_limit(ParentUuid, FilteredChildrenWhiteList, Size + NonNegOffset) of
        {ok, [], _} ->
            {ok, [], #list_extended_info{is_finished = true}};
        {ok, ValidLinks, ListedLinksCount} ->
            prepare_list_result(ValidLinks, undefined, ListedLinksCount < Size + NonNegOffset,
                lists:last(ValidLinks), NonNegOffset);
        {error, _} = Error ->
            Error
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
-spec check_name_and_get_conflicting_files(forest(), link_name(), link_target(), od_provider:id(), od_space:id(),
    od_space:id() | undefined) -> ok | {conflicting, TaggedName :: file_meta:name(), Conflicts :: [link()]}.
check_name_and_get_conflicting_files(ParentUuid, FileName, FileUuid, FileProviderId, Scope, RemoteScope) ->
    case file_meta_forest:get_local_or_remote(ParentUuid, FileName, Scope, RemoteScope) of
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

%% @private
-spec fold(datastore:ctx(), forest(), fold_fun(), fold_acc(), datastore_fold_opts()) ->
    {{ok, fold_acc()}, datastore_links_iter:token() | undefined} | {error, term()}.
fold(Ctx, ParentUuid, Fun, AccIn, Opts) ->
    case datastore_model:fold_links(Ctx, ParentUuid, all, Fun, AccIn, Opts) of
        {ok, Result} -> {{ok, Result}, undefined};
        Other -> Other
    end.


%% @private
-spec get_links_up_to_limit(forest(), [link_name()], limit()) ->
    {ok, [internal_link()], non_neg_integer()} | {error, term()}.
get_links_up_to_limit(ParentUuid, LinkNames, Limit) ->
    lists_utils:foldl_while(fun
        (_, {ok, AccLinks, ListedCount}) when ListedCount == Limit ->
            {halt, {ok, AccLinks, ListedCount}};
        ({ok, L}, {ok, AccLinks, ListedCount}) ->
            {cont, {ok, AccLinks ++ L, ListedCount + 1}};
        ({error, not_found}, {ok, AccLinks, ListedCount}) ->
            {cont, {ok, AccLinks, ListedCount}};
        ({error, _} = Error, _) ->
            {halt, Error}
    end, {ok, [], 0}, get(ParentUuid, all, LinkNames)).


%% @private
-spec adjust_opts_for_offset(list_opts()) -> list_opts().
adjust_opts_for_offset(#{offset := Offset, size := Size} = Opts) when is_integer(Offset) andalso Offset > 0 ->
    maps:remove(offset, Opts#{size => Size + Offset});
adjust_opts_for_offset(Opts) ->
    Opts.


%% @private
-spec prepare_list_result([internal_link()], datastore_list_token() | undefined, boolean(),
    internal_link() | undefined, offset()) -> {ok, [link()], list_extended_info()}.
prepare_list_result(Links, TokenOrUndefined, ListedLessThanRequested, LastLink, InitialOffset) ->
    ExtendedInfo = case TokenOrUndefined of
        #link_token{} = Token -> #list_extended_info{
            datastore_token = Token,
            is_finished = Token#link_token.is_last
        };
        undefined -> #list_extended_info{
            is_finished = ListedLessThanRequested
        }
    end,
    ExtendedInfo2 = case LastLink of
        #link{name = Name, tree_id = Tree} ->
            ExtendedInfo#list_extended_info{last_name = Name, last_tree = Tree};
        undefined ->
            ExtendedInfo
    end,
    CroppedLinks = lists:sublist(Links, max(1, InitialOffset + 1), length(Links)),
    {ok, tag_ambiguous(CroppedLinks), ExtendedInfo2}.


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


%% @private
-spec calculate_suffix_length(group()) -> non_neg_integer().
calculate_suffix_length(Group) ->
    RemoteTreeIds = get_remote_tree_ids(Group),
    LongestCommonPrefixLen = binary:longest_common_prefix(RemoteTreeIds),
    max(?MINIMAL_TREE_ID_SUFFIX_LEN, LongestCommonPrefixLen + 1).


%% @private
-spec get_remote_tree_ids(group()) -> tree_ids().
get_remote_tree_ids(Group) ->
    LocalTreeId = ?LOCAL_TREE_ID,
    lists:filtermap(fun(#link{tree_id = TreeId}) ->
        case TreeId =/= LocalTreeId of
            true -> {true, TreeId};
            false -> false
        end
    end, Group).


%% @private
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


%% @private
-spec tag_remote_files(internal_link(), tree_id(), non_neg_integer()) -> link().
tag_remote_files(#link{name = Name, target = FileUuid, tree_id = LocalTreeId}, LocalTreeId, _) ->
    ?LINK(Name, FileUuid);
tag_remote_files(#link{name = Name, target = FileUuid, tree_id = RemoteTreeId}, _LocalTreeId, SuffixLength) ->
    Suffix = binary_part(RemoteTreeId, 0, min(SuffixLength, byte_size(RemoteTreeId))),
    ?LINK(?CONFLICTING_LOGICAL_FILE_NAME(Name, Suffix), FileUuid).
