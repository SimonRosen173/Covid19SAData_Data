/*Requires node.js to be run*/

const rp = require('request-promise');
// const url = 'https://en.wikipedia.org/wiki/List_of_Presidents_of_the_United_States';
const nicd_media_alert_url = "https://www.nicd.ac.za/media/alerts/";
// const url = "https://www.nicd.ac.za/latest-confirmed-cases-of-covid-19-in-south-africa-07-jan-2021/";
const ch = require('cheerio');

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

function extract_from_page(url){
    rp(url)
        .then(function(html){
            // console.log("\n-----------------------\n");
            console.log(url);
            let headings = ch('h1',html);
            let first_h1_text = headings[0].childNodes[0].data;
            console.log(first_h1_text);
            // extract date
            let date_regex = /[0-9]{2} [a-zA-Z]{3} [0-9]{4}/g; // e.g. for 01 May 2020
            let date_text = first_h1_text.match(date_regex)[0];
            console.log(date_text);

            // get tables
            let tables = ch('table',html);

            // Cases
            let cases_raw_data = extract_from_table(tables[0]);
            let cases_data = drop_col(cases_raw_data, 2);
            cases_data[0][1] = "Cases";

            console.log("###");
            print_data(cases_data);

            // Tests
            let tests_raw_data = extract_from_table(tables[1]);
            let tests_data = drop_cols(tests_raw_data, [2,3,4]);
            console.log("###");
            print_data(tests_data);

            // Deaths, Recoveries & Active
            let deaths_recovered_raw_data = extract_from_table(tables[2]);
            // console.log(deaths_recovered_raw_data);
            let deaths_recovered_data = drop_col(deaths_recovered_raw_data, 3);
            deaths_recovered_data[0][0] = "Province";
            deaths_recovered_data[deaths_recovered_data.length-1][0]= "Total";
            console.log("###");
            print_data(deaths_recovered_data);

            console.log("###");
            console.log("SUCCESS");
        })
        .catch(function (err){
            console.log("An error occured: " + err);
        });
}

// Get latest media alert for 'latest confirmed cases' from nicd media alerts
rp(nicd_media_alert_url)
    .then(function(html){
        let alert_urls_dicts = [];
        let alert_a_list = ch('article > div > div > h3 > a', html);

        for (let i = 0; i<alert_a_list.length; i++){
            let curr_heading_text = alert_a_list[i].children[0].data;
            curr_heading_text = curr_heading_text.replaceAll(/\t|\n|\r'/g,'');
            if (curr_heading_text.toLowerCase().includes('latest confirmed cases')){
                let date_regex = /[0-9]{2} [a-zA-Z]{3} [0-9]{4}/g; // e.g. for 01 May 2020
                let date_text = curr_heading_text.match(date_regex)[0];
                alert_urls_dicts.push({"date":date_text, "url":alert_a_list[i].attribs.href});
            }
        }
        // console.log(alert_urls_dicts);
        // for (let i = 0; i< alert_urls_dicts.length; i++){
        //     extract_from_page(alert_urls_dicts[i]['url']);
        // }
        // Latest media release
        extract_from_page(alert_urls_dicts[0]['url']);
    })
    .catch(function (err){
        console.log(err);
    });

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