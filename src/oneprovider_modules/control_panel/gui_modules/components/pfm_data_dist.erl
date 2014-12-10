%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains code for file_manager's data distribution view component.
%% It consists of several functions that render n2o elements and implement logic.
%% @end
%% ===================================================================
-module(pfm_data_dist).

-include("oneprovider_modules/control_panel/common.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([init/0, data_distribution_panel/1, on_resize_js/0]).
-export([hide_ddist_panel/1, hide_all_ddist_panels/0, refresh_ddist_panels/0]).
-export([toggle_ddist_view/3, refresh_view/4, sync_file/2, expel_file/2]).

% Macros used to generate IDs of certain elements
-define(DIST_PANEL_ID(FileID), <<"dd_", (gui_str:unicode_list_to_binary(FileID))/binary>>).
-define(SHOW_DIST_PANEL_ID(FileID), <<"show_dd_", (gui_str:unicode_list_to_binary(FileID))/binary>>).
-define(HIDE_DIST_PANEL_ID(FileID), <<"hide_dd_", (gui_str:unicode_list_to_binary(FileID))/binary>>).
-define(CANVAS_ID(FileID, ProviderID), <<"canvas_", (gui_str:unicode_list_to_binary(FileID))/binary, "_", ProviderID/binary>>).
-define(SYNC_BUTTON_ID(FileID, ProviderID), <<"sync_", (gui_str:unicode_list_to_binary(FileID))/binary, "_", ProviderID/binary>>).
-define(EXPEL_BUTTON_ID(FileID, ProviderID), <<"expel_", (gui_str:unicode_list_to_binary(FileID))/binary, "_", ProviderID/binary>>).

% Reference to comet pid that handles updates of data distribution view
-define(DD_COMET_PID, dd_comet_pid).

% How often should file's distribution status be refreshed
-define(REFRESH_PERIOD, 1500).


%% ====================================================================
%% API functions
%% ====================================================================

%% init/0
%% ====================================================================
%% @doc Initializes state in file_manager's comet process memory.
%% @end
-spec init() -> term().
%% ====================================================================
init() ->
    set_displayed_ddist_panels([]).


%% data_distribution_panel/2
%% ====================================================================
%% @doc Renders data distribution panel to be concatenated to every file row in list view.
%% @end
-spec data_distribution_panel(FullPath :: binary()) -> [term()].
%% ====================================================================
data_distribution_panel(FilePath) ->
    FullPath = fs_interface:get_full_file_path(FilePath),
    FileID = fs_interface:get_file_uuid(FullPath),
    {FileSize, FileBlocks} = fs_interface:get_file_block_map(FullPath),
    ShowDDistID = ?SHOW_DIST_PANEL_ID(FileID),
    % Item won't hightlight if the link is clicked.
    gui_jq:bind_element_click(ShowDDistID, <<"function(e) { e.stopPropagation(); }">>),
    HideDDistID = ?HIDE_DIST_PANEL_ID(FileID),
    % Item won't hightlight if the link is clicked.
    gui_jq:bind_element_click(HideDDistID, <<"function(e) { e.stopPropagation(); }">>),
    DDistPanelID = ?DIST_PANEL_ID(FileID),
    {ShowLinkClass, HideLinkClass, DDistPanelClass, Body} =
        case lists:keyfind(FullPath, 1, get_displayed_ddist_panels()) of
            false ->
                {
                    <<"glyph-link show-on-parent-hover ddist-show-button">>,
                    <<"glyph-link hidden ddist-hide-button">>,
                    <<"ddist-panel display-none">>,
                    <<"">>
                };
            _ ->
                {
                    <<"glyph-link hidden ddist-show-button">>,
                    <<"glyph-link ddist-hide-button">>,
                    <<"ddist-panel">>,
                    render_table(FileID, FullPath, FileSize, FileBlocks)
                }
        end,
    [
        #link{id = ShowDDistID, postback = {action, ?MODULE, toggle_ddist_view, [FileID, FullPath, true]},
            title = <<"Data distribution (advanced)">>, class = ShowLinkClass,
            body = #span{class = <<"icomoon-earth">>}},
        #link{id = HideDDistID, postback = {action, ?MODULE, toggle_ddist_view, [FileID, FullPath, false]},
            title = <<"Hide data distribution view">>, class = HideLinkClass,
            body = #span{class = <<"icomoon-minus4">>}},
        #panel{class = <<"clearfix">>},
        #panel{id = DDistPanelID, class = DDistPanelClass, body = Body}
    ].


%% toggle_ddist_view/4
%% ====================================================================
%% @doc Function evaluated in postback that shows or hides the data distribution panel.
%% @end
-spec toggle_ddist_view(FileID :: string(), FullPath :: string(), Flag :: boolean()) -> term().
%% ====================================================================
toggle_ddist_view(FileID, FullPath, Flag) ->
    {FileSize, FileBlocks} = fs_interface:get_file_block_map(FullPath),
    ShowDDistID = ?SHOW_DIST_PANEL_ID(FileID),
    HideDDistID = ?HIDE_DIST_PANEL_ID(FileID),
    DDistPanelID = ?DIST_PANEL_ID(FileID),
    case Flag of
        true ->
            gui_jq:update(DDistPanelID, render_table(FileID, FullPath, FileSize, FileBlocks)),
            gui_jq:remove_class(HideDDistID, <<"hidden">>),
            gui_jq:remove_class(ShowDDistID, <<"show-on-parent-hover">>),
            gui_jq:slide_down(DDistPanelID, 400);
        false ->
            gui_jq:add_class(ShowDDistID, <<"show-on-parent-hover">>),
            gui_jq:add_class(HideDDistID, <<"hidden">>),
            gui_jq:slide_up(DDistPanelID, 200)
    end,
    CurrentDisplayed = lists:keydelete(FileID, 1, get_displayed_ddist_panels()),
    NewDisplayed = case Flag of
                       true -> [{FileID, FullPath, md5_hash(FileSize, FileBlocks)} | CurrentDisplayed];
                       false -> CurrentDisplayed
                   end,
    set_displayed_ddist_panels(NewDisplayed).


%% refresh_view/4
%% ====================================================================
%% @doc Refreshes the distribution status of given file.
%% @end
-spec refresh_view(FileID :: string(), FullPath :: string(), FileSize :: integer(),
    FileBlocks :: [{ProviderID :: binary(), ProviderName :: binary(), [integer()]}]) -> term().
%% ====================================================================
refresh_view(FileID, FullPath, FileSize, FileBlocks) ->
    gui_jq:update(?DIST_PANEL_ID(FileID), render_table(FileID, FullPath, FileSize, FileBlocks)).


%% render_table/4
%% ====================================================================
%% @doc Renders the table with distribution status for given file.
%% @end
-spec render_table(FileID :: string(), FullPath :: string(), FileSize :: integer(),
    FileBlocks :: [{ProviderID :: binary(), ProviderName :: binary(), [integer()]}]) -> term().
%% ====================================================================
render_table(FileID, _FullPath, FileSize, FileBlocks) ->
    gui_jq:wire("$(window).resize();"),
    [
        #p{body = <<"File distribution:">>, class = <<"ddist-header">>},
        #table{class = <<"ddist-table">>,
            body = #tbody{body = [
                lists:map(
                    fun({ProviderID, ProviderName, ProvBytes, BlockList}) ->
                        CanvasID = ?CANVAS_ID(FileID, ProviderID),
                        SyncButtonID = ?SYNC_BUTTON_ID(FileID, ProviderID),
                        ExpelButtonID = ?EXPEL_BUTTON_ID(FileID, ProviderID),
                        % Item won't hightlight if the link is clicked.
                        gui_jq:bind_element_click(SyncButtonID, <<"function(e) { e.stopPropagation(); }">>),
                        gui_jq:bind_element_click(ExpelButtonID, <<"function(e) { e.stopPropagation(); }">>),
                        JSON = rest_utils:encode_to_json([{<<"file_size">>, FileSize}, {<<"chunks">>, BlockList}]),
                        gui_jq:wire(<<"new FileChunksBar(document.getElementById('", CanvasID/binary, "'), '", JSON/binary, "');">>),
                        Percentage = case FileSize of
                                         0 -> 0;
                                         _ -> ProvBytes * 10000 div FileSize
                                     end,
                        PercentageBin = gui_str:format_bin("~b.~b%", [Percentage div 100, Percentage rem 100]),
                        #tr{cells = [
                            #td{body = ProviderName, class = <<"ddist-provider">>},
                            #td{body = PercentageBin, class = <<"ddist-percentage">>},
                            #td{body = #canvas{id = CanvasID, class = <<"ddist-canvas">>}},
                            % TODO Not yet supported
%%                             #td{body = #link{id = SyncButtonID, postback = {action, ?MODULE, sync_file, [FullPath, ProviderID]},
%%                                 title = <<"Issue full synchronization">>, class = <<"glyph-link ddist-button">>,
%%                                 style = <<"z-index: 2;">>, body = #span{class = <<"icomoon-spinner6">>}}},
                            % TODO Not yet supported
%%                             #td{body = #link{id = ExpelButtonID, postback = {action, ?MODULE, expel_file, [FilePath, ProviderID]},
%%                                 title = <<"Expel all chunks from this provider">>, class = <<"glyph-link ddist-button">>,
%%                                 body = #span{class = <<"icomoon-blocked">>}}},
                            #td{body = #link{id = SyncButtonID,
                                title = <<"Issue full synchronization">>, class = <<"glyph-link-gray ddist-button">>,
                                body = #span{class = <<"icomoon-spinner6">>}}},
                            #td{body = #link{id = ExpelButtonID,
                                title = <<"Expel all chunks from this provider">>, class = <<"glyph-link-gray ddist-button">>,
                                body = #span{class = <<"icomoon-blocked">>}}}
                        ]}
                    end, FileBlocks)
            ]}
        }
    ].


%% sync_file/3
%% ====================================================================
%% @doc Issues full synchronization (downloading all the blocks) of selected file.
%% @end
-spec sync_file(FullPath :: binary(), ProviderID :: string()) -> term().
%% ====================================================================
sync_file(FullPath, ProviderID) ->
    fs_interface:issue_remote_file_synchronization(FullPath, ProviderID).


%% expel_file/2
%% ====================================================================
%% @doc Issues removal of all file blocks from selected provider.
%% @end
-spec expel_file(FullPath :: binary(), ProviderID :: string()) -> term().
%% ====================================================================
expel_file(_FilePath, _ProviderID) ->
    % TODO Not yet supported
    ok.


%% on_resize_js/0
%% ====================================================================
%% @doc JavaScript snippet that handles scaling of data distibution panel.
%% @end
-spec on_resize_js() -> binary().
%% ====================================================================
on_resize_js() ->
    <<"if ($($('.list-view-name-header')[0]).width() < 300) { ",
    "$('.ddist-percentage').hide(); $('.ddist-canvas').css('width', '50px'); $('.ddist-provider').css('max-width', '70px'); } ",
    "else if ($($('.list-view-name-header')[0]).width() < 400) {",
    "$('.ddist-percentage').hide(); $('.ddist-canvas').css('width', '80px'); $('.ddist-provider').css('max-width', '150px'); } else ",
    "{ $('.ddist-percentage').show(); $('.ddist-canvas').css('width', '125px'); $('.ddist-provider').css('max-width', '150px'); }">>.


%% refresh_ddist_panels/0
%% ====================================================================
%% @doc Refreshes data distribution panels (if they have changed).
%% @end
-spec refresh_ddist_panels() -> term().
%% ====================================================================
refresh_ddist_panels() ->
    lists:foreach(
        fun({FileID, FullPath, MD5Hash}) ->
            {FileSize, FileBlocks} = fs_interface:get_file_block_map(FullPath),
            case md5_hash(FileSize, FileBlocks) of
                MD5Hash ->
                    ok;
                NewHash ->
                    refresh_view(FileID, FullPath, FileSize, FileBlocks),
                    set_displayed_ddist_panels([{FileID, FullPath, NewHash}] ++ lists:keydelete(FileID, 1, get_displayed_ddist_panels()))
            end
        end, get_displayed_ddist_panels()).


hide_ddist_panel(FilePath) ->
    FullPath = fs_interface:get_full_file_path(FilePath),
    FileID = fs_interface:get_file_uuid(FullPath),
    set_displayed_ddist_panels(lists:keydelete(FileID, 1, get_displayed_ddist_panels())).


%% hide_all_ddist_panels/0
%% ====================================================================
%% @doc Removes all data distribution panels from process memory (none will be displayed after page update).
%% @end
-spec hide_all_ddist_panels() -> term().
%% ====================================================================
hide_all_ddist_panels() ->
    put(ddd_panels, []).


%% set_displayed_ddist_panels/1
%% ====================================================================
%% @doc Remembers what file have distribution status displayed.
%% @end
-spec set_displayed_ddist_panels([{FileID :: string(), FullPath :: string(), MD5 :: binary()}]) -> binary().
%% ====================================================================
set_displayed_ddist_panels(List) ->
    put(ddd_panels, List).


%% get_displayed_ddist_panels/0
%% ====================================================================
%% @doc Returns stored list of displayed data distribution panels.
%% @end
-spec get_displayed_ddist_panels() -> [{FileID :: string(), FullPath :: string(), MD5 :: binary()}].
%% ====================================================================
get_displayed_ddist_panels() ->
    get(ddd_panels).


%% md5_hash/2
%% ====================================================================
%% @doc Returns stored list of displayed data distribution panels.
%% @end
-spec md5_hash(FileSize :: integer(),
    FileBlocks :: [{ProviderID :: binary(), ProviderName :: binary(), integer(), [integer()]}]) -> binary().
%% ====================================================================
md5_hash(FileSize, FileBlocks) ->
    erlang:md5(term_to_binary({FileSize, FileBlocks})).