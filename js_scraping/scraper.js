/*Requires node.js to be run*/

const rp = require('request-promise');
// const url = 'https://en.wikipedia.org/wiki/List_of_Presidents_of_the_United_States';
const nicd_media_alert_url = "https://www.nicd.ac.za/media/alerts/";
const problem_url = "https://www.nicd.ac.za/latest-confirmed-cases-of-covid-19-in-south-africa-28-jan-2021/";
const ch = require('cheerio');

let date_regex = /[0-9]{1,2} [a-zA-Z]{3,} [0-9]{4}/g; // e.g. for 01 May 2020 or 1 January 2020

// drop 'column' from 2d array based off given index
function drop_col(table, col_ind){
    for (let i = 0; i<table.length; i++){
        table[i].splice(col_ind, 1); // remove element at col_ind
    }
    return table;
}

function drop_cols(table, col_inds){
    for (let i = 0; i<table.length; i++){
        for (let j = col_inds.length - 1; j>=0; j--){
            table[i].splice(col_inds[j], 1); // remove element at col_inds[j]
        }
    }
    return table;
}

function extract_from_table(table){
    let rows = ch('tr',table);
    let all_cells_txt = [];

    if (rows.length > 0) {
        let tbl_headings_ch = ch('td', rows[0]);
        let tbl_headings_txt = [];
        for (let i = 0; i<tbl_headings_ch.length; i++){
            tbl_headings_txt.push(ch("*",tbl_headings_ch[i]).text());
        }
        all_cells_txt.push(tbl_headings_txt);

        for (let i = 1; i<rows.length; i++){ // rows.length
            let curr_cells_ch = ch('td', rows[i]);
            let curr_cells_txt = [];
            // console.log(curr_cells_ch.text());
            for (let j = 0; j<curr_cells_ch.length; j++){
                // try with cheerio else -> //
                let curr_cell_ch_txt = ch("*",curr_cells_ch[j]).text();

                if (curr_cell_ch_txt === "" || curr_cell_ch_txt === null){
                    if (curr_cells_ch[j].childNodes.length > 0){
                        curr_cells_txt.push(curr_cells_ch[j].childNodes[0].data);
                    } else {
                        curr_cells_txt.push("");
                    }
                } else {
                    curr_cells_txt.push(curr_cell_ch_txt); // ch("*",curr_cells_ch[j]).text()
                }
            }
            all_cells_txt.push(curr_cells_txt);
        }
    }
    // look at last cell
    if (all_cells_txt[all_cells_txt.length - 1].length !== all_cells_txt[0].length){
        let last_row_cells = all_cells_txt[all_cells_txt.length - 1];
        let last_row_txt = last_row_cells.join('\xa0\xa0');
        let rows_regex = /[\s|\u00A0]{2,}|[\n\r\t]/; // 2 or more whitespace characters // [\s|\u00A0]{2,}
        let new_last_row_cells = last_row_txt.split(rows_regex);
        all_cells_txt[all_cells_txt.length - 1] =  new_last_row_cells;
    }

    // let tmp_arr = ["1\xa0\xa0\xa0",2," 3"];
    // let tmp_arr_txt = tmp_arr.join('\xa0');
    // console.log(tmp_arr_txt);
    // let tmp_regex = /[\s|\u00A0]{2,}/;
    // let tmp_arr_split = tmp_arr_txt.split(tmp_regex);
    // console.log(tmp_arr_split);

    // let last_rows = last_row_txt.split();

    // console.log(all_cells_txt);

    return all_cells_txt;
}

function format_val(value){
    return value.toString().replaceAll(" ","");
}

function print_data(data){
    for (let i = 0; i<data.length; i++){
        let log_txt = data[i][0];
        for (let j = 1; j<data[i].length; j++){
            if (i === 0) {
                log_txt += "," + data[i][j];
            } else {
                log_txt += "," + format_val(data[i][j]);
            }
        }
        console.log(log_txt);
    }
}

function format_date(date_str){
    if (date_str.match(date_regex)!== null) {
        let date_arr = date_str.split(" ");
        let day = date_arr[0];
        let month = date_arr[1];
        let year = date_arr[2];
        if (day.length < 2) {
            day = "0" + day;
        }
        if (month.length !== 3) {
            month = month.slice(0, 3);
        }
        date_str = day + " " + month + " " + year;
        return date_str;
    } else {
        return "";
    }
}

function extract_from_page(url){
    rp(url)
        .then(function(html){
            // console.log("\n-----------------------\n");
            // let out_str = url + "\n";
            console.log(url);
            let h1s = ch('h1',html);
            let first_h1_text = h1s[0].childNodes[0].data;
            console.log(first_h1_text);
            // out_str += first_h1_text + "\n";
            // extract date
            // let date_regex = /[0-9]{1,2} [a-zA-Z]{3,} [0-9]{4}/g; // e.g. for 01 May 2020 or 1 January 2020
            // let date_text = first_h1_text.match(date_regex)[0];
            // let date_text = "";
            // let h3s = ch('h3', html);
            // for (let i = 0; i<h3s.length; i++){
            //     let curr_h3_txt = h3s[i].childNodes[0].data;
            //     let regex_match = curr_h3_txt.match(date_regex);
            //     if (regex_match!==null){
            //         // date_text = regex_match[0];
            //         date_text = format_date(regex_match[0]);
            //         // let date_arr = date_text.split(" ");
            //         // let day = date_arr[0];
            //         // let month = date_arr[1];
            //         // let year = date_arr[2];
            //         // if (day.length < 2){
            //         //     day = "0" + day;
            //         // }
            //         // if (month.length !== 3){
            //         //     month = month.slice(0,3);
            //         // }
            //         // date_text = day + " " + month + " " + year;
            //         // convert to correct format
            //         // Do Stuff
            //         break;
            //     }
            // }

            // console.log(h3s)

            // console.log(date_text);
            // out_str+=date_text+"\n";

            // get tables
            let tables = ch('table',html);

            // CASES
            let cases_raw_data = extract_from_table(tables[0]);
            // get date - assuming it is from second column heading (format will probs change randomly)
            let raw_str = cases_raw_data[0][1];
            let date_match = raw_str.match(date_regex);
            let date_str = "";
            if (date_match !== null){
                date_str = format_date(date_match[0]);
            }
            console.log(date_str);

            let cases_data = drop_col(cases_raw_data, 2);
            cases_data[0][1] = "Cases";

            console.log("###");
            print_data(cases_data);

            // TESTS
            let tests_raw_data = extract_from_table(tables[1]);
            let tests_data = drop_cols(tests_raw_data, [2,3,4]);
            // out_str += "###\n";
            console.log("###");
            print_data(tests_data);

            // Deaths, Recoveries & Active
            let deaths_recovered_raw_data = extract_from_table(tables[2]);
            // console.log(deaths_recovered_raw_data);
            let deaths_recovered_data = drop_col(deaths_recovered_raw_data, 3);
            // deaths_recovered_data[0][0] = "Province";
            deaths_recovered_data[0] = ["Province","Deaths","Recoveries"];
            deaths_recovered_data[deaths_recovered_data.length-1][0]= "Total";
            // out_str+="###\n";
            console.log("###");
            print_data(deaths_recovered_data);

            // out_str+="###\nSUCCESS\n";
            console.log("###");
            console.log("SUCCESS");

        })
        .catch(function (err){
            console.log("An error occured: " + err);
        });
}

function sort_dates(obj1, obj2){
    let date1 = new Date(obj1.date);
    let date2 = new Date(obj2.date);

    // date 1 before date 2 => return 1
    // date 1 after date 2 => return -1
    // date 1 equal to date 2 => return 0

    if (date1<date2){
        return 1;
    } else if(date1>date2){
        return -1;
    } else {
        return 0;
    }
}

function scrape(no_sources){
    // Get latest media alert for 'latest confirmed cases' from nicd media alerts
    rp(nicd_media_alert_url)
        .then(function(html){
            let alert_urls_dicts = [];
            let alert_a_list = ch('article > div > div > h3 > a', html);
            let alert_urls_list = [];

            for (let i = 0; i<alert_a_list.length; i++){
                let curr_heading_text = alert_a_list[i].children[0].data;
                curr_heading_text = curr_heading_text.replaceAll(/\t|\n|\r'/g,'');
                if (curr_heading_text.toLowerCase().includes('latest confirmed cases')){
                    let date_regex = /[0-9]{1,2} [a-zA-Z]{3} [0-9]{4}/g; // e.g. for 01 May 2020
                    let date_text = curr_heading_text.match(date_regex)[0];
                    alert_urls_dicts.push({"date":date_text, "url":alert_a_list[i].attribs.href});
                    alert_urls_list.push(alert_a_list[i].attribs.href);
                }
            }
            // alert_urls_dicts.sort(sort_dates);
            // console.log(alert_urls_dicts);
            // for (let i = 0; i< alert_urls_dicts.length; i++){
            //     extract_from_page(alert_urls_dicts[i]['url']);
            // }
            // Latest media release
            console.log(no_sources)
            for (let i =0; i<no_sources; i++){
                extract_from_page(alert_urls_dicts[i]['url']);
            }
        })
        .catch(function (err){
            console.log(err);
        });
}

function main(){
    let no_sources = 1; // default
    if (process.argv.length >= 2 && process.argv[2] !== undefined){
        no_sources = process.argv[2];
    }

    // console.log(no_sources);
    scrape(no_sources);
    // extract_from_page(problem_url);
}

main();
// console.log(new Date("9 Jan 2021") > new Date("9 Feb 2021"));

// let tmp_arr = ["1\xa0\xa0\xa0",2,"\t3"];
// let tmp_arr_txt = tmp_arr.join('\xa0');
// console.log(tmp_arr_txt);
// let tmp_regex = /[\s|\u00A0]{2,}|[\n\r\t]/;
// let tmp_arr_split = tmp_arr_txt.split(tmp_regex);
// console.log(tmp_arr_split);

// puppeteer
//     .launch()
//     .then(function(browser){
//         return browser.newPage();
//     })
//     .then(function(page){
//         return page.goto(url)
//             .then(function(){
//                 return page.content();
//             });
//     })
//     .then(function (html){
//         console.log(html);
//     })
//     .catch(function (err){
//         //
//     });


// rp(url)
//     .then(function(html){
//         //success!
//         console.log(html);
//     })
//     .catch(function(err){
//         //handle error
//     });