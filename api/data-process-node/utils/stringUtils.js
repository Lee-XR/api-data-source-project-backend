// Remove all white space
function removeWhiteSpace(text) {
    return text.replace(/\s/g, '');
}

function removeAllSymbols(text) {
    return text.replace(/[^\w\s]|_/g, '');
}

// Reformat phone number string
function formatPhoneNumber(phone_number) {
    if (phone_number === '' || phone_number === null) {
        return '';
    }

    if (phone_number.includes('-') || phone_number.includes(' ')) {
        phone_number = removeWhiteSpace(phone_number);
        phone_number = phone_number.replace('-', '');
    }

    const notDigitRegex = /[^0-9]/;
    if (notDigitRegex.test(phone_number)) {
        return '';
    }

    const allZeroRegex = /^0+$/;
    if (allZeroRegex.test(phone_number)) {
        return '';
    }

    if (phone_number.length < 10 || phone_number.length > 11) {
        return '';
    }
    
    return phone_number;
}

// Filter selected words from text string
function filterWordsFromString(text, filter_array) {
    let filteredText = text;
    for (const word of filter_array) {
        const index = text.indexOf(word);
        if (index >= 0) {
            filteredText = filteredText.replaceAll(word, "");
        }
    }

    return filteredText;
}

export { removeWhiteSpace, removeAllSymbols, formatPhoneNumber, filterWordsFromString }