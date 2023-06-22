// Remove all white space
function removeWhiteSpace(text) {
    return text.replace(/\s/g, '');
}

// Clean phone number string to standard format
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

export { removeWhiteSpace, formatPhoneNumber }