(function ($) {
    let functionUrlPresign = localStorage.getItem("functionUrlPresignReview");
    if (functionUrlPresign) {
        $("#functionUrlPresignReview").val(functionUrlPresign);
    }

    let functionUrlList = localStorage.getItem("functionUrlListReview");
    if (functionUrlList) {
        $("#functionUrlListReview").val(functionUrlList);
    }

    let reviewItemTemplate = Handlebars.compile($("#review-item-template").html());

    $("#configForm").submit(async function (event) {
        event.preventDefault();

        let action = $(this).find("button[type=submit]:focus").attr('name') || event.originalEvent.submitter.getAttribute('name');

        if (action === "load") {
            let baseUrl = `${document.location.protocol}//${document.location.host}`;
            if (baseUrl.indexOf("file://") >= 0) baseUrl = `http://localhost:4566`;
            baseUrl = baseUrl.replace("://webapp.s3.", "://").replace("://webapp.s3-website.", "://");
            const headers = {authorization: "AWS4-HMAC-SHA256 Credential=test/20231004/us-east-1/lambda/aws4_request, ..."};

            const loadUrl = async (funcName, resultElement) => {
                const url = `${baseUrl}/2021-10-31/functions/${funcName}/urls`;
                const result = await $.ajax({url, headers}).promise();
                const funcUrl = JSON.parse(result).FunctionUrlConfigs[0].FunctionUrl;
                $(`#${resultElement}`).val(funcUrl);
                localStorage.setItem(resultElement, funcUrl);
            }

            await loadUrl("presignReview", "functionUrlPresignReview");
            await loadUrl("listReviews", "functionUrlListReview");
            alert("Function URL configurations loaded");

        } else if (action === "save") {
            localStorage.setItem("functionUrlPresignReview", $("#functionUrlPresignReview").val());
            localStorage.setItem("functionUrlListReview", $("#functionUrlListReview").val());
            alert("Configuration saved");
        } else if (action === "clear") {
            localStorage.removeItem("functionUrlPresignReview");
            localStorage.removeItem("functionUrlListReview");
            $("#functionUrlPresignReview").val("");
            $("#functionUrlListReview").val("");
            alert("Configuration cleared");
        }
    });

    $("#uploadForm").submit(function (event) {
        $("#uploadForm button").addClass('disabled');
        event.preventDefault();

        let fileName = $("#customFile").val().replace(/C:\\fakepath\\/i, '');
        let functionUrlPresign = $("#functionUrlPresignReview").val();

        if (!fileName || !functionUrlPresign) {
            alert("Missing file or function URL.");
            return;
        }

        let urlToCall = functionUrlPresign + "/" + fileName;

        $.ajax({
            url: urlToCall,
            success: function (data) {
                let fields = data['fields'];
                let formData = new FormData();

                Object.entries(fields).forEach(([field, value]) => {
                    formData.append(field, value);
                });

                const fileElement = document.querySelector("#customFile");
                formData.append("file", fileElement.files[0]);

                $.ajax({
                    type: "POST",
                    url: data['url'],
                    data: formData,
                    processData: false,
                    contentType: false,
                    success: function () {
                        alert("Upload successful!");
                        updateReviewList();
                    },
                    error: function () {
                        alert("Upload failed! Check logs.");
                    },
                    complete: function () {
                        $("#uploadForm button").removeClass('disabled');
                    }
                });
            },
            error: function () {
                alert("Error getting pre-signed URL. Check logs.");
                $("#uploadForm button").removeClass('disabled');
            }
        });
    });

    function updateReviewList() {
        let listUrl = $("#functionUrlListReview").val();
        if (!listUrl) {
            alert("Please set the function URL of the list Lambda");
            return;
        }

        $.ajax({
            url: listUrl,
            success: function (response) {
                $('#reviewsContainer').empty();
                response.forEach(function (item) {
                    let cardHtml = reviewItemTemplate(item);
                    $("#reviewsContainer").append(cardHtml);
                });
            },
            error: function () {
                alert("Error loading review list. Check logs.");
            }
        });
    }

    $("#updateReviewListButton").click(updateReviewList);

    if (functionUrlList) updateReviewList();
})(jQuery);
